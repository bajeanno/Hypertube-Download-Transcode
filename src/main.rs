use axum::Router;
use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{delete, post};
use serde::Deserialize;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpListener;

mod transcode;

const MEDIA_PATH: &str = "/Users/mini/hypertube/media";

struct AppError(anyhow::Error);

#[allow(dead_code)]
struct AppState;

impl From<anyhow::Error> for AppError {
    fn from(value: anyhow::Error) -> Self {
        Self(value)
    }
}

impl IntoResponse for AppError {
    fn into_response(self) -> axum::response::Response {
        (StatusCode::INTERNAL_SERVER_ERROR, self.0.to_string()).into_response()
    }
}

#[derive(Deserialize)]
struct DownloadRequest {
    preferred_language: String,
    torrent_id: usize,
}

#[allow(dead_code)]
struct DownloadId {
    id: usize,
}

#[allow(unused)]
async fn transcode_delete(
    State(state): State<Arc<AppState>>,
    Path(id): Path<usize>,
) -> Result<impl IntoResponse, AppError> {
    Ok(format!("parsed: {id}"))
}

fn clean_output_dir(output_dir: &std::path::Path) -> Result<(), Box<dyn std::error::Error>> {
    let patterns = ["m3u8", "ts", "vtt", "json", "tmp", "part"];

    for entry in std::fs::read_dir(output_dir)? {
        let path = entry?.path();

        if path.is_file()
            && path
                .extension()
                .and_then(|ext| ext.to_str())
                .is_some_and(|ext| patterns.contains(&ext))
        {
            std::fs::remove_file(&path)?;
        }
    }

    Ok(())
}

async fn transcode(
    State(_state): State<Arc<AppState>>,
    Query(req): Query<DownloadRequest>,
    body: Body,
) -> Result<impl IntoResponse, AppError> {
    let preferred_lang = req.preferred_language.clone();
    let output_dir =
        PathBuf::from(MEDIA_PATH).join(std::path::Path::new(&req.torrent_id.to_string()));
    let _ = clean_output_dir(&output_dir);

    transcode::convert_pipe_hls(body.into_data_stream(), output_dir, preferred_lang)
        .await
        .map_err(|err| AppError(anyhow::anyhow!("{err}")))?;

    Ok(())
}

async fn not_found() -> Response {
    (StatusCode::NOT_FOUND, "404 not found").into_response()
}

#[tokio::main]
async fn main() {
    let state = Arc::new(AppState);

    let app: Router = Router::new()
        .route("/transcode", post(transcode))
        .route("/transcode/{id}", delete(transcode_delete))
        .fallback(not_found)
        .with_state(state);

    let addr: &str = "0.0.0.0:3000";
    let listener = TcpListener::bind(addr).await.unwrap();
    println!("Server listening on {addr}");
    axum::serve(listener, app).await.unwrap();
}
