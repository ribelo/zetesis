use chrono::Utc;
use serde_json::{Map as JsonMap, Value as JsonValue, json};

use crate::pipeline::Silo;
use crate::pipeline::structured::{DecisionResult, StructuredDecision};

const DOC_TYPE_DOC: &str = "doc";
const DOC_TYPE_CHUNK: &str = "chunk";

#[derive(Debug, Clone, Copy)]
pub enum IngestStatus {
    Pending,
    Indexed,
    Error,
}

impl IngestStatus {
    fn as_str(self) -> &'static str {
        match self {
            IngestStatus::Pending => "pending",
            IngestStatus::Indexed => "indexed",
            IngestStatus::Error => "error",
        }
    }
}

#[derive(Debug, Clone)]
pub struct IngestMetadata<'a> {
    pub status: IngestStatus,
    pub embedder_key: Option<&'a str>,
    pub chunk_count: usize,
    pub error_message: Option<&'a str>,
}

impl<'a> IngestMetadata<'a> {
    pub fn pending() -> Self {
        Self {
            status: IngestStatus::Pending,
            embedder_key: None,
            chunk_count: 0,
            error_message: None,
        }
    }

    pub fn indexed(embedder_key: &'a str, chunk_count: usize) -> Self {
        Self {
            status: IngestStatus::Indexed,
            embedder_key: Some(embedder_key),
            chunk_count,
            error_message: None,
        }
    }

    pub fn errored(message: &'a str) -> Self {
        Self {
            status: IngestStatus::Error,
            embedder_key: None,
            chunk_count: 0,
            error_message: Some(message),
        }
    }
}

#[derive(Debug, Clone)]
pub struct PreparedChunkRecord {
    pub ord: u32,
    pub body: String,
    pub record: JsonMap<String, JsonValue>,
}

#[derive(Debug, Clone)]
pub struct PreparedDocumentRecords {
    pub doc: JsonMap<String, JsonValue>,
    pub chunks: Vec<PreparedChunkRecord>,
}

pub fn build_document_payload(
    decision: &StructuredDecision,
    silo: Silo,
    doc_id: &str,
    embedder_key: &str,
    ingest: IngestMetadata<'_>,
) -> Result<PreparedDocumentRecords, serde_json::Error> {
    debug_assert!(!doc_id.is_empty());
    debug_assert!(decision.chunks.len() < u32::MAX as usize);

    let doc_record = build_doc_record(decision, silo, doc_id, embedder_key, ingest)?;
    let chunks = make_chunk_records(decision, silo, doc_id);
    Ok(PreparedDocumentRecords {
        doc: doc_record,
        chunks,
    })
}

pub fn build_doc_record(
    decision: &StructuredDecision,
    silo: Silo,
    doc_id: &str,
    embedder_key: &str,
    ingest: IngestMetadata<'_>,
) -> Result<JsonMap<String, JsonValue>, serde_json::Error> {
    debug_assert!(decision.summary_short.len() > 0);
    debug_assert!(decision.panel.len() <= 16);

    let panel_names: Vec<String> = decision
        .panel
        .iter()
        .map(|member| member.name.clone())
        .collect();
    let panel_roles: Vec<String> = decision
        .panel
        .iter()
        .map(|member| member.role.as_ref().to_string())
        .collect();
    let panel_summary = json!({
        "names": panel_names.clone(),
        "roles": panel_roles.clone(),
    });
    let interested_bidders = decision.parties.interested_bidders.clone();
    let procurement_cpv = decision.procurement.cpv_codes.clone();
    let issues_keys: Vec<String> = decision
        .issues
        .iter()
        .map(|issue| issue.issue_key.as_ref().to_string())
        .collect();

    let mut record = JsonMap::new();
    record.insert(
        "id".to_string(),
        JsonValue::String(format!("{}-{}-doc", silo.slug(), doc_id)),
    );
    record.insert(
        "silo".to_string(),
        JsonValue::String(silo.slug().to_string()),
    );
    record.insert(
        "doc_type".to_string(),
        JsonValue::String(DOC_TYPE_DOC.to_string()),
    );
    record.insert("doc_id".to_string(), JsonValue::String(doc_id.to_string()));
    record.insert(
        "summary_short".to_string(),
        JsonValue::String(decision.summary_short.clone()),
    );
    record.insert("panel_names".to_string(), json!(panel_names));
    record.insert("panel_roles".to_string(), json!(panel_roles));
    record.insert("panel".to_string(), panel_summary.clone());
    record.insert(
        "parties_contracting_authority".to_string(),
        JsonValue::String(decision.parties.contracting_authority.clone()),
    );
    record.insert(
        "parties_appellant".to_string(),
        JsonValue::String(decision.parties.appellant.clone()),
    );
    record.insert(
        "parties_awardee".to_string(),
        decision
            .parties
            .awardee
            .as_ref()
            .map(|value| JsonValue::String(value.clone()))
            .unwrap_or(JsonValue::Null),
    );
    record.insert(
        "parties_interested_bidders".to_string(),
        json!(interested_bidders),
    );
    record.insert(
        "procurement_subject".to_string(),
        decision
            .procurement
            .subject
            .as_ref()
            .map(|value| JsonValue::String(value.clone()))
            .unwrap_or(JsonValue::Null),
    );
    record.insert(
        "procurement_procedure_type".to_string(),
        decision
            .procurement
            .procedure_type
            .map(|value| JsonValue::String(value.as_ref().to_string()))
            .unwrap_or(JsonValue::Null),
    );
    record.insert(
        "procurement_tender_id".to_string(),
        decision
            .procurement
            .tender_id
            .as_ref()
            .map(|value| JsonValue::String(value.clone()))
            .unwrap_or(JsonValue::Null),
    );
    record.insert("procurement_cpv_codes".to_string(), json!(procurement_cpv));
    record.insert(
        "identifiers_kio_docket".to_string(),
        json!(decision.identifiers.kio_docket.clone()),
    );
    record.insert(
        "identifiers_saos_id".to_string(),
        decision
            .identifiers
            .saos_id
            .as_ref()
            .map(|value| JsonValue::String(value.clone()))
            .unwrap_or(JsonValue::Null),
    );
    record.insert(
        "decision_date".to_string(),
        JsonValue::String(decision.decision.date.clone()),
    );
    record.insert(
        "decision_result".to_string(),
        JsonValue::String(decision_result_value(decision.decision.result)),
    );
    record.insert("issues_keys".to_string(), json!(issues_keys));
    record.insert("structured".to_string(), serde_json::to_value(decision)?);
    record.insert(
        "ingest".to_string(),
        JsonValue::Object(build_ingest_object(ingest)),
    );
    let mut vectors = JsonMap::new();
    vectors.insert(embedder_key.to_string(), JsonValue::Null);
    record.insert("_vectors".to_string(), JsonValue::Object(vectors));
    Ok(record)
}

fn make_chunk_records(
    decision: &StructuredDecision,
    silo: Silo,
    doc_id: &str,
) -> Vec<PreparedChunkRecord> {
    debug_assert!(!decision.decision.date.is_empty());
    debug_assert!(decision.chunks.len() > 0);

    let base_identifier = json!({
        "kio_docket": &decision.identifiers.kio_docket,
        "saos_id": decision.identifiers.saos_id.as_ref(),
    });

    let base_decision = json!({
        "date": &decision.decision.date,
        "result": decision_result_value(decision.decision.result),
    });

    let parties = json!({
        "contracting_authority": &decision.parties.contracting_authority,
        "appellant": &decision.parties.appellant,
        "awardee": decision.parties.awardee.as_ref(),
        "interested_bidders": &decision.parties.interested_bidders,
    });

    let procurement = json!({
        "subject": decision.procurement.subject.as_ref(),
        "procedure_type": decision.procurement.procedure_type.map(|pt| pt.as_ref().to_string()),
        "tender_id": decision.procurement.tender_id.as_ref(),
        "cpv_codes": &decision.procurement.cpv_codes,
    });
    let panel_names: Vec<String> = decision
        .panel
        .iter()
        .map(|member| member.name.clone())
        .collect();
    let panel_roles: Vec<String> = decision
        .panel
        .iter()
        .map(|member| member.role.as_ref().to_string())
        .collect();
    let panel = json!({
        "names": panel_names.clone(),
        "roles": panel_roles.clone(),
    });

    let issues: Vec<JsonValue> = decision
        .issues
        .iter()
        .map(|iss| {
            json!({
                "issue_key": iss.issue_key.as_ref(),
                "confidence": iss.confidence,
            })
        })
        .collect();
    let issues_keys: Vec<String> = decision
        .issues
        .iter()
        .map(|issue| issue.issue_key.as_ref().to_string())
        .collect();

    let mut out = Vec::with_capacity(decision.chunks.len());
    for chunk in &decision.chunks {
        let mut record = JsonMap::new();
        record.insert(
            "id".to_string(),
            JsonValue::String(format!(
                "{}-{}-chunk-{}",
                silo.slug(),
                doc_id,
                chunk.position
            )),
        );
        record.insert(
            "silo".to_string(),
            JsonValue::String(silo.slug().to_string()),
        );
        record.insert(
            "doc_type".to_string(),
            JsonValue::String(DOC_TYPE_CHUNK.to_string()),
        );
        record.insert("doc_id".to_string(), JsonValue::String(doc_id.to_string()));
        record.insert(
            "ord".to_string(),
            JsonValue::Number(serde_json::Number::from(u64::from(chunk.position))),
        );
        record.insert(
            "section".to_string(),
            chunk
                .section
                .as_ref()
                .map(|value| JsonValue::String(value.clone()))
                .unwrap_or(JsonValue::Null),
        );
        record.insert("content".to_string(), JsonValue::String(chunk.body.clone()));
        record.insert("decision".to_string(), base_decision.clone());
        record.insert("identifiers".to_string(), base_identifier.clone());
        record.insert("parties".to_string(), parties.clone());
        record.insert("procurement".to_string(), procurement.clone());
        record.insert("panel".to_string(), panel.clone());
        record.insert("issues".to_string(), JsonValue::Array(issues.clone()));
        record.insert("panel_names".to_string(), json!(panel_names));
        record.insert("panel_roles".to_string(), json!(panel_roles));
        record.insert(
            "parties_contracting_authority".to_string(),
            JsonValue::String(decision.parties.contracting_authority.clone()),
        );
        record.insert(
            "parties_appellant".to_string(),
            JsonValue::String(decision.parties.appellant.clone()),
        );
        record.insert(
            "parties_awardee".to_string(),
            decision
                .parties
                .awardee
                .as_ref()
                .map(|value| JsonValue::String(value.clone()))
                .unwrap_or(JsonValue::Null),
        );
        record.insert(
            "parties_interested_bidders".to_string(),
            json!(decision.parties.interested_bidders),
        );
        record.insert(
            "procurement_subject".to_string(),
            decision
                .procurement
                .subject
                .as_ref()
                .map(|value| JsonValue::String(value.clone()))
                .unwrap_or(JsonValue::Null),
        );
        record.insert(
            "procurement_procedure_type".to_string(),
            decision
                .procurement
                .procedure_type
                .map(|value| JsonValue::String(value.as_ref().to_string()))
                .unwrap_or(JsonValue::Null),
        );
        record.insert(
            "procurement_tender_id".to_string(),
            decision
                .procurement
                .tender_id
                .as_ref()
                .map(|value| JsonValue::String(value.clone()))
                .unwrap_or(JsonValue::Null),
        );
        record.insert(
            "procurement_cpv_codes".to_string(),
            json!(decision.procurement.cpv_codes),
        );
        record.insert(
            "identifiers_kio_docket".to_string(),
            json!(decision.identifiers.kio_docket),
        );
        record.insert(
            "identifiers_saos_id".to_string(),
            decision
                .identifiers
                .saos_id
                .as_ref()
                .map(|value| JsonValue::String(value.clone()))
                .unwrap_or(JsonValue::Null),
        );
        record.insert(
            "decision_date".to_string(),
            JsonValue::String(decision.decision.date.clone()),
        );
        record.insert(
            "decision_result".to_string(),
            JsonValue::String(decision_result_value(decision.decision.result)),
        );
        record.insert("issues_keys".to_string(), json!(issues_keys));

        out.push(PreparedChunkRecord {
            ord: chunk.position as u32,
            body: chunk.body.clone(),
            record,
        });
    }

    out
}

fn build_ingest_object(meta: IngestMetadata<'_>) -> JsonMap<String, JsonValue> {
    let mut ingest = JsonMap::new();
    ingest.insert(
        "status".to_string(),
        JsonValue::String(meta.status.as_str().to_string()),
    );
    ingest.insert(
        "updated_at".to_string(),
        JsonValue::String(Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Millis, true)),
    );
    ingest.insert(
        "embedder_key".to_string(),
        meta.embedder_key
            .map(|value| JsonValue::String(value.to_string()))
            .unwrap_or(JsonValue::Null),
    );
    ingest.insert(
        "chunk_count".to_string(),
        JsonValue::Number(serde_json::Number::from(meta.chunk_count as u64)),
    );
    ingest.insert(
        "error".to_string(),
        meta.error_message
            .map(|msg| JsonValue::String(msg.to_string()))
            .unwrap_or(JsonValue::Null),
    );
    ingest
}

fn decision_result_value(result: DecisionResult) -> String {
    match result {
        DecisionResult::Unknown => "unknown".to_string(),
        other => other.as_ref().to_string(),
    }
}
