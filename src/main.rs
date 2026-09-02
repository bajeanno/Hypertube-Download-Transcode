use anyhow::anyhow;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::{Json, Router};
use librqbit::{AddTorrent, AddTorrentResponse, Session};
use serde::Deserialize;
use std::sync::Arc;
use tokio::net::TcpListener;

mod downloads;
use downloads::TorrentList;

struct AppError(anyhow::Error);

struct AppState {
    session: Arc<Session>,
    torrents_list: Arc<TorrentList>,
}

struct Index {
    index: usize,
}

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
    filename: String,
}

struct DownloadId {
    id: usize,
}

#[allow(unreachable_code)]
async fn download(
    State(state): State<Arc<AppState>>,
    Json(req): Json<DownloadRequest>,
) -> Result<impl IntoResponse, AppError> {
    let resp = state
        .session
        .add_torrent(AddTorrent::from_local_filename(&req.filename)?, None)
        .await?;
    match resp {
        AddTorrentResponse::Added(id, handle) | AddTorrentResponse::AlreadyManaged(id, handle) => {
            state
                .torrents_list
                .insert(id, handle.output_folder().into());
            tokio::spawn(async move { handle.wait_until_completed().await });
            Ok(format!("started torrent #{id}"))
        }
        _ => Err(anyhow!("torrent added but not initialized").into()),
    }
}

async fn stop() {
    println!("stop todo");
}

#[allow(unused)]
async fn index(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i32>,
) -> Result<axum::response::Json<Index>, AppError> {
    todo!()
}

#[tokio::main]
async fn main() {
    let session = Session::new("/Users/bolide/Downloads/torrents".into())
        .await
        .expect("cannot start rqbit session");
    let downloads = TorrentList::new();

    let state = Arc::new(AppState {
        session,
        torrents_list: downloads,
    });
    let app: Router = Router::new()
        .route("/download", post(download))
        .route("/stop", post(stop))
        .route("/index/{id}", get(index))
        .with_state(state);

    let addr: &str = "0.0.0.0:3000";
    let listener = TcpListener::bind(addr).await.unwrap();
    println!("Server listening on {addr}");
    axum::serve(listener, app).await.unwrap();
}
