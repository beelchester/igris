use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use datafusion::arrow::json::ArrayWriter;
use serde_json::json;
mod store;
use store::{LogFilter, LogStore};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt};

#[derive(Clone)]
struct AppState {
    logs: LogStore,
}

#[tokio::main]
async fn main() {
    tracing_subscriber::registry()
        .with(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| {
                format!("{}=debug,tower_http=debug", env!("CARGO_CRATE_NAME")).into()
            }),
        )
        .with(tracing_subscriber::fmt::layer())
        .init();

    let state = AppState {
        logs: LogStore::new().unwrap(),
    };

    let app = Router::new()
        .route("/health", get(health))
        .route("/api/v1/logs", post(log).get(search))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind("127.0.0.1:3000")
        .await
        .unwrap();
    tracing::debug!("listening on {}", listener.local_addr().unwrap());
    axum::serve(listener, app).await.unwrap();
}

async fn health() -> impl IntoResponse {
    StatusCode::OK
}

const DEFAULT_LIMIT: usize = 100;
const MAX_LIMIT: usize = 1000;
const MAX_BATCH: usize = 10_000;

async fn log(State(state): State<AppState>, Json(input): Json<LogInput>) -> impl IntoResponse {
    let events = match input {
        LogInput::One(event) => vec![event],
        LogInput::Many(events) => events,
    };

    if events.is_empty() {
        return error_response(vec!["batch is empty".to_string()]);
    }
    if events.len() > MAX_BATCH {
        return error_response(vec![format!("batch exceeds {MAX_BATCH} events")]);
    }

    let errors: Vec<String> = events
        .iter()
        .enumerate()
        .flat_map(|(i, event)| {
            event
                .validate()
                .into_iter()
                .map(move |e| format!("[{i}] {e}"))
        })
        .collect();
    if !errors.is_empty() {
        return error_response(errors);
    }

    let accepted = events.len();
    if let Err(err) = state.logs.append(&events).await {
        tracing::error!("failed to store logs: {err}");
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "errors": ["failed to store logs"] })),
        );
    }
    (StatusCode::ACCEPTED, Json(json!({ "accepted": accepted })))
}

#[derive(Deserialize)]
struct SearchParams {
    service: Option<String>,
    level: Option<Level>,
    from: Option<i64>,
    to: Option<i64>,
    q: Option<String>,
    limit: Option<usize>,
}

async fn search(
    State(state): State<AppState>,
    Query(params): Query<SearchParams>,
) -> (StatusCode, Json<serde_json::Value>) {
    let limit = params.limit.unwrap_or(DEFAULT_LIMIT);
    if limit == 0 || limit > MAX_LIMIT {
        return error_response(vec![format!("limit must be between 1 and {MAX_LIMIT}")]);
    }
    if let (Some(from), Some(to)) = (params.from, params.to) {
        if from >= to {
            return error_response(vec!["from must be less than to".to_string()]);
        }
    }

    let filter = LogFilter {
        service: params.service,
        level: params.level,
        from: params.from,
        to: params.to,
        q: params.q,
        limit,
    };

    match encode(&state.logs, &filter).await {
        Ok(logs) => (StatusCode::OK, Json(json!({ "count": logs.len(), "logs": logs }))),
        Err(err) => {
            tracing::error!("failed to query logs: {err}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(json!({ "errors": ["failed to query logs"] })),
            )
        }
    }
}

async fn encode(
    store: &LogStore,
    filter: &LogFilter,
) -> Result<Vec<serde_json::Value>, Box<dyn std::error::Error>> {
    let batches = store.query(filter).await?;
    let mut writer = ArrayWriter::new(Vec::new());
    for batch in &batches {
        writer.write(batch)?;
    }
    writer.finish()?;
    let buf = writer.into_inner();
    if buf.is_empty() {
        return Ok(Vec::new());
    }
    Ok(serde_json::from_slice(&buf)?)
}

fn error_response(errors: Vec<String>) -> (StatusCode, Json<serde_json::Value>) {
    (StatusCode::BAD_REQUEST, Json(json!({ "errors": errors })))
}

#[derive(Deserialize)]
#[serde(untagged)]
enum LogInput {
    One(LogEvent),
    Many(Vec<LogEvent>),
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
enum Level {
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LogEvent {
    timestamp: i64,
    service: String,
    level: Level,
    message: String,
}

impl LogEvent {
    fn validate(&self) -> Vec<&'static str> {
        let mut errors = Vec::new();
        if self.timestamp <= 0 {
            errors.push("timestamp must be positive epoch milliseconds");
        }
        if self.service.trim().is_empty() {
            errors.push("service must not be empty");
        }
        if self.message.is_empty() {
            errors.push("message must not be empty");
        }
        errors
    }
}

impl Level {
    fn as_str(&self) -> &'static str {
        match self {
            Level::Trace => "trace",
            Level::Debug => "debug",
            Level::Info => "info",
            Level::Warn => "warn",
            Level::Error => "error",
        }
    }
}
