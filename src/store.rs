use crate::{Level, LogEvent};
use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::catalog::MemTable;
use datafusion::error::Result;
use datafusion::execution::context::SessionContext;
use datafusion::functions::expr_fn::contains;
use datafusion::prelude::{col, lit};
use std::sync::Arc;

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

#[derive(Clone)]
pub struct LogStore {
    ctx: SessionContext,
}

impl LogStore {
    pub fn new() -> Result<Self> {
        let ctx = SessionContext::new();
        let table = MemTable::try_new(schema(), vec![vec![]])?;
        ctx.register_table(TABLE, Arc::new(table))?;
        Ok(Self { ctx })
    }

    pub async fn append(&self, events: &[LogEvent]) -> Result<()> {
        let batch = to_batch(events)?;
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
