use crate::{Level, LogEvent};
use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::MemTable;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::functions_aggregate::expr_fn::count;
use datafusion::functions::expr_fn::contains;
use datafusion::prelude::{col, lit};
use datafusion::error::DataFusionError;
use datafusion::parquet::arrow::ArrowWriter;
use datafusion::prelude::ParquetReadOptions;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const TABLE: &str = "logs";

pub fn schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("timestamp", DataType::Int64, false),
        Field::new("service", DataType::Utf8, false),
        Field::new("level", DataType::Utf8, false),
        Field::new("message", DataType::Utf8, false),
    ]))
}

pub struct LogFilter {
    pub service: Option<String>,
    pub level: Option<Level>,
    pub from: Option<i64>,
    pub to: Option<i64>,
    pub q: Option<String>,
    pub limit: usize,
}

const SWEEP_INTERVAL: Duration = Duration::from_secs(60);

#[derive(Clone)]
pub struct LogStore {
    ctx: SessionContext,
    dir: Arc<PathBuf>,
    retention_ms: Option<i64>,
    seq: Arc<AtomicU64>,
}

impl LogStore {
    pub async fn open(dir: PathBuf, retention_ms: Option<i64>) -> Result<Self> {
        std::fs::create_dir_all(&dir)?;

        let mut files = Vec::new();
        for entry in std::fs::read_dir(&dir)? {
            let path = entry?.path();
            match path.extension().and_then(|e| e.to_str()) {
                Some("parquet") => files.push(path.to_string_lossy().into_owned()),
                Some("tmp") => std::fs::remove_file(&path)?,
                _ => {}
            }
        }
        files.sort();

        let ctx = SessionContext::new();
        let mut batches = Vec::new();
        if !files.is_empty() {
            let schema = schema();
            let options = ParquetReadOptions::default().schema(&schema);
            batches = ctx.read_parquet(files, options).await?.collect().await?;
        }
        let table = MemTable::try_new(schema(), vec![batches])?;
        ctx.register_table(TABLE, Arc::new(table))?;

        let store = Self {
            ctx,
            dir: Arc::new(dir),
            retention_ms,
            seq: Arc::new(AtomicU64::new(0)),
        };
        store.sweep().await?;
        Ok(store)
    }

    pub fn spawn_retention(&self) {
        if self.retention_ms.is_none() {
            return;
        }
        let store = self.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(SWEEP_INTERVAL);
            ticker.tick().await;
            loop {
                ticker.tick().await;
                if let Err(err) = store.sweep().await {
                    tracing::error!("retention sweep failed: {err}");
                }
            }
        });
    }

    pub async fn sweep(&self) -> Result<()> {
        let Some(retention_ms) = self.retention_ms else {
            return Ok(());
        };
        let cutoff = now_ms() - retention_ms;

        for entry in std::fs::read_dir(self.dir.as_path())? {
            let path = entry?.path();
            if path.extension().and_then(|e| e.to_str()) != Some("parquet") {
                continue;
            }
            if file_max_timestamp(&path).is_some_and(|max| max < cutoff) {
                std::fs::remove_file(&path)?;
            }
        }

        self.ctx
            .sql(&format!("delete from {TABLE} where timestamp < {cutoff}"))
            .await?
            .collect()
            .await?;
        Ok(())
    }

    pub async fn append(&self, events: &[LogEvent]) -> Result<()> {
        let batch = to_batch(events)?;
        let min = events.iter().map(|e| e.timestamp).min().unwrap_or(0);
        let max = events.iter().map(|e| e.timestamp).max().unwrap_or(0);
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        let name = format!("{min}-{max}-{}-{seq}.parquet", now_nanos());
        let path = self.dir.join(name);

        let to_write = batch.clone();
        tokio::task::spawn_blocking(move || write_parquet(&path, &to_write))
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))??;

        self.ctx
            .read_batch(batch)?
            .write_table(TABLE, Default::default())
            .await?;
        Ok(())
    }

    pub async fn query(&self, filter: &LogFilter) -> Result<Vec<RecordBatch>> {
        let mut df = self.ctx.table(TABLE).await?;
        if let Some(service) = &filter.service {
            df = df.filter(col("service").eq(lit(service.as_str())))?;
        }
        if let Some(level) = filter.level {
            df = df.filter(col("level").eq(lit(level.as_str())))?;
        }
        if let Some(from) = filter.from {
            df = df.filter(col("timestamp").gt_eq(lit(from)))?;
        }
        if let Some(to) = filter.to {
            df = df.filter(col("timestamp").lt(lit(to)))?;
        }
        if let Some(q) = &filter.q {
            df = df.filter(contains(col("message"), lit(q.as_str())))?;
        }
        df.sort(vec![col("timestamp").sort(false, false)])?
            .limit(0, Some(filter.limit))?
            .collect()
            .await
    }

    pub async fn stats(
        &self,
        service: Option<&str>,
        from: Option<i64>,
        to: Option<i64>,
        bucket_ms: i64,
    ) -> Result<Vec<RecordBatch>> {
        let mut df = self.ctx.table(TABLE).await?;
        if let Some(service) = service {
            df = df.filter(col("service").eq(lit(service)))?;
        }
        if let Some(from) = from {
            df = df.filter(col("timestamp").gt_eq(lit(from)))?;
        }
        if let Some(to) = to {
            df = df.filter(col("timestamp").lt(lit(to)))?;
        }
        let bucket = (col("timestamp") / lit(bucket_ms) * lit(bucket_ms)).alias("bucket");
        df.aggregate(vec![bucket, col("level")], vec![count(lit(1)).alias("count")])?
            .sort(vec![
                col("bucket").sort(true, false),
                col("level").sort(true, false),
            ])?
            .collect()
            .await
    }
}

fn to_batch(events: &[LogEvent]) -> Result<RecordBatch> {
    let timestamp: ArrayRef = Arc::new(Int64Array::from_iter_values(
        events.iter().map(|e| e.timestamp),
    ));
    let service: ArrayRef = Arc::new(StringArray::from_iter_values(
        events.iter().map(|e| e.service.as_str()),
    ));
    let level: ArrayRef = Arc::new(StringArray::from_iter_values(
        events.iter().map(|e| e.level.as_str()),
    ));
    let message: ArrayRef = Arc::new(StringArray::from_iter_values(
        events.iter().map(|e| e.message.as_str()),
    ));
    Ok(RecordBatch::try_new(
        schema(),
        vec![timestamp, service, level, message],
    )?)
}

fn write_parquet(path: &Path, batch: &RecordBatch) -> Result<()> {
    let tmp = path.with_extension("tmp");
    let file = std::fs::File::create(&tmp)?;
    let mut writer = ArrowWriter::try_new(file, batch.schema(), None)?;
    writer.write(batch)?;
    writer.close()?;
    std::fs::rename(&tmp, path)?;
    Ok(())
}

fn file_max_timestamp(path: &Path) -> Option<i64> {
    let stem = path.file_stem()?.to_str()?;
    stem.split('-').nth(1)?.parse().ok()
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

fn now_nanos() -> u128 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos())
        .unwrap_or(0)
}
