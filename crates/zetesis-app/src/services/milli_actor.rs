use std::thread::{self, JoinHandle};

use flume::{Receiver, Sender};
use serde_json::{Map as JsonMap, Value as JsonValue};
use tokio::task;
use tracing::error;

use crate::index::writer::{IndexWriteError, IndexWriter};
use crate::services::context::{PipelineError, PipelineResult};

pub struct MilliActorHandle {
    tx: Sender<MilliCmd>,
    join: Option<JoinHandle<()>>,
}

enum MilliCmd {
    Upsert {
        records: Vec<JsonMap<String, JsonValue>>,
        resp: Sender<Result<milli::update::DocumentAdditionResult, IndexWriteError>>,
    },
    Stop,
}

impl MilliActorHandle {
    pub fn spawn(index: milli::Index, capacity: usize) -> Self {
        let (tx, rx) = flume::bounded::<MilliCmd>(capacity.max(1));
        let join = Some(thread::spawn(move || run_actor(rx, index)));
        Self { tx, join }
    }

    pub async fn upsert(
        &self,
        records: Vec<JsonMap<String, JsonValue>>,
    ) -> PipelineResult<milli::update::DocumentAdditionResult> {
        let (resp_tx, resp_rx) = flume::bounded(1);
        self.tx
            .send_async(MilliCmd::Upsert {
                records,
                resp: resp_tx,
            })
            .await
            .map_err(|e| PipelineError::message(format!("actor send failed: {e}")))?;
        resp_rx
            .recv_async()
            .await
            .map_err(|e| PipelineError::message(format!("actor recv failed: {e}")))?
            .map_err(PipelineError::from)
    }

    pub async fn upsert_one(
        &self,
        record: JsonMap<String, JsonValue>,
    ) -> PipelineResult<milli::update::DocumentAdditionResult> {
        self.upsert(vec![record]).await
    }

    pub async fn shutdown(mut self) {
        if let Err(err) = self.tx.send_async(MilliCmd::Stop).await {
            error!("failed to send stop to milli actor: {err}");
        }
        if let Some(join) = self.join.take() {
            let _ = task::spawn_blocking(move || {
                let _ = join.join();
            })
            .await;
        }
    }
}

impl Drop for MilliActorHandle {
    fn drop(&mut self) {
        if let Some(join) = self.join.take() {
            let tx = self.tx.clone();
            thread::spawn(move || {
                let _ = tx.send(MilliCmd::Stop);
                let _ = join.join();
            });
        }
    }
}

fn run_actor(rx: Receiver<MilliCmd>, index: milli::Index) {
    let writer = IndexWriter::new(index);
    while let Ok(cmd) = rx.recv() {
        match cmd {
            MilliCmd::Upsert { records, resp } => {
                let res = writer.upsert(&records);
                let _ = resp.send(res);
            }
            MilliCmd::Stop => break,
        }
    }
}
