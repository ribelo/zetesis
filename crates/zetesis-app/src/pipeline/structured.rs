//! Structured representation of KIO decisions extracted from OCR output.
//!
//! These data types model the minimum viable contract we expect from the
//! structured extraction stage. They stay pure, provide JSON schema generation
//! for prompting, and expose validation so downstream code can refuse malformed
//! payloads before writing to storage or search indices.

use std::{
    borrow::Cow,
    fmt::{self, Display},
    sync::OnceLock,
};

use chrono::NaiveDate;
use regex::Regex;
use schemars::{JsonSchema, Schema, SchemaGenerator};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use strum::{AsRefStr, EnumIter};
use thiserror::Error;

/// Canonical structured payload for a single KIO decision.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct StructuredDecision {
    /// External identifiers tied to this document (e.g., docket numbers).
    pub identifiers: Identifiers,
    /// Decision metadata such as the date, result, and issued orders.
    pub decision: DecisionInfo,
    /// Members of the adjudicating panel in order of appearance.
    pub panel: Vec<PanelMember>,
    /// Parties involved in the proceedings (zamawiający, odwołujący, etc.).
    pub parties: Parties,
    /// Procurement context extracted from the reasoning, when available.
    #[serde(default, skip_serializing_if = "Procurement::is_empty")]
    pub procurement: Procurement,
    /// Financial orders with payer/recipient flows.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub costs: Vec<CostOrder>,
    /// Statutory provisions explicitly cited in the document.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub statutes_cited: Vec<StatuteMention>,
    /// High-level issues describing the subject of the dispute.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub issues: Vec<IssueTag>,
    /// Semantic chunks covering the entire decision text (body text only) in order.
    #[serde(default)]
    pub chunks: Vec<SemanticChunk>,
    /// Short summary (1–2 sentences) capturing the essence of the ruling.
    #[serde(default)]
    pub summary_short: String,
}

impl StructuredDecision {
    /// Validate semantic constraints beyond plain JSON typing.
    pub fn validate(&self) -> Result<(), StructuredValidationError> {
        let mut issues = Vec::new();

        if self.panel.is_empty() {
            issues.push("panel must contain at least one member".to_string());
        }

        if let Err(err) = validate_date(&self.decision.date) {
            issues.push(format!("decision.date {err}"));
        }

        if self.decision.result == DecisionResult::Unknown {
            issues.push("decision.result must be a recognised value".to_string());
        }

        if self.parties.contracting_authority.trim().is_empty() {
            issues.push("parties.contracting_authority must not be empty".to_string());
        }
        if self.parties.appellant.trim().is_empty() {
            issues.push("parties.appellant must not be empty".to_string());
        }

        if let Some(procurement) = self.procurement.as_option() {
            if let Err(errs) = procurement.validate() {
                issues.extend(errs.into_iter().map(|msg| format!("procurement.{msg}")));
            }
        }

        for (idx, cost) in self.costs.iter().enumerate() {
            if cost.amount < 0.0 {
                issues.push(format!("costs[{idx}].amount must be >= 0"));
            }
            if !is_valid_currency(&cost.currency) {
                issues.push(format!(
                    "costs[{idx}].currency must be ISO 4217 uppercase, got '{}'",
                    cost.currency
                ));
            }
        }

        for (idx, statute) in self.statutes_cited.iter().enumerate() {
            if statute.article.trim().is_empty() {
                issues.push(format!("statutes_cited[{idx}].article must not be empty"));
            }
        }

        for (idx, mention) in self.issues.iter().enumerate() {
            if mention.confidence > 100 {
                issues.push(format!("issues[{idx}].confidence must be <= 100"));
            }
        }

        if self.chunks.is_empty() {
            issues.push("chunks must contain at least one semantic chunk".to_string());
        } else {
            let mut expected_position: u16 = 1;
            for (idx, chunk) in self.chunks.iter().enumerate() {
                if chunk.position != expected_position {
                    issues.push(format!(
                        "chunks[{idx}].position must equal {expected_position}"
                    ));
                }
                if let Some(section) = chunk.section.as_ref() {
                    if section.trim().is_empty() {
                        issues.push(format!("chunks[{idx}].section must not be empty"));
                    }
                }
                if chunk.body.trim().is_empty() {
                    issues.push(format!("chunks[{idx}].body must not be empty"));
                }
                expected_position = expected_position.saturating_add(1);
            }
        }

        if self.summary_short.trim().is_empty() {
            issues.push("summary_short must not be empty".to_string());
        }

        if issues.is_empty() {
            Ok(())
        } else {
            Err(StructuredValidationError { issues })
        }
    }
}

/// External identifiers for a decision.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct Identifiers {
    /// Docket numbers issued by KIO (e.g., "KIO/UZP 113/08").
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub kio_docket: Vec<String>,
    /// Identifier assigned by the SAOS portal, if present.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub saos_id: Option<String>,
}

/// Core decision outcome metadata.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DecisionInfo {
    /// Date of the decision in ISO 8601 (YYYY-MM-DD).
    pub date: String,
    /// Result of the appeal.
    pub result: DecisionResult,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    /// Operative orders appended to the verdict.
    pub orders: Vec<String>,
}

/// Member of the adjudicating panel.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PanelMember {
    /// Full name exactly as written in the decision.
    pub name: String,
    /// Role of the member (chair, rapporteur, etc.).
    pub role: PanelRole,
}

/// Parties involved in the proceedings.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct Parties {
    /// Contracting authority (zamawiający).
    pub contracting_authority: String,
    /// Appellant (odwołujący).
    pub appellant: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Awardee whose offer was selected, when available.
    pub awardee: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    /// Other interested bidders admitted to the proceedings.
    pub interested_bidders: Vec<String>,
}

/// Procurement metadata associated with the decision.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct Procurement {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Subject of the procurement as described in the ruling.
    pub subject: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    #[schemars(schema_with = "schema_nullable_procedure_type")]
    /// Procedure type applied by the contracting authority.
    pub procedure_type: Option<ProcedureType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Tender identifier or internal case number, if provided.
    pub tender_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    /// CPV codes linked to the procurement scope. Accepts either the canonical
    /// 8-digit form (e.g., "39120000") or the common hyphenated form with a
    /// trailing check digit (e.g., "39120000-3").
    pub cpv_codes: Vec<String>,
}

impl Procurement {
    fn is_empty(&self) -> bool {
        self.subject.is_none()
            && self.procedure_type.is_none()
            && self.tender_id.is_none()
            && self.cpv_codes.is_empty()
    }

    fn as_option(&self) -> Option<&Self> {
        if self.is_empty() { None } else { Some(self) }
    }

    fn validate(&self) -> Result<(), Vec<String>> {
        let mut issues = Vec::new();
        for (idx, cpv) in self.cpv_codes.iter().enumerate() {
            if !is_valid_cpv(cpv) {
                issues.push(format!(
                    "cpv_codes[{idx}] must match NNNNNNNN or NNNNNNNN-X, got '{}'",
                    cpv
                ));
            }
        }
        if issues.is_empty() {
            Ok(())
        } else {
            Err(issues)
        }
    }
}

/// Cost allocation ordered by the decision.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct CostOrder {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Entity ordered to pay the costs.
    pub payer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Recipient of the awarded costs.
    pub recipient: Option<String>,
    /// Amount of costs expressed as a number.
    pub amount: f64,
    /// ISO 4217 currency code for the amount.
    pub currency: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional description of the cost type.
    pub label: Option<String>,
}

/// Mention of a statute in the reasoning.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct StatuteMention {
    /// Code of the statute (e.g., PZP).
    pub code: StatuteCode,
    /// Article number within the statute.
    pub article: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Paragraph within the article.
    pub paragraph: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Point within the paragraph.
    pub point: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Optional quotation or snippet referencing the statute.
    pub quote: Option<String>,
}

/// Topical categorisation of the dispute.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct IssueTag {
    /// Label describing the issue area.
    pub issue_key: IssueKey,
    #[serde(default)]
    /// Confidence score (0-100) for the assigned label.
    pub confidence: u8,
}

/// Semantic representation of a contiguous text fragment.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct SemanticChunk {
    /// 1-based order of the chunk as it appears in the document.
    pub position: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    /// Logical section label (e.g., "sentencja", "uzasadnienie").
    pub section: Option<String>,
    /// Full body text for the chunk.
    pub body: String,
}

/// Result of the decision regarding the appeal.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, EnumIter, AsRefStr)]
#[serde(rename_all = "snake_case")]
pub enum DecisionResult {
    /// Appeal dismissed; zamawiający prevails.
    Dismissed,
    /// Appeal upheld in full.
    Upheld,
    /// Appeal upheld in part.
    PartiallyUpheld,
    /// Proceedings discontinued (e.g., withdrawal of appeal).
    Discontinued,
    /// Previous decision annulled.
    Annulled,
    #[strum(disabled)]
    #[serde(other)]
    Unknown,
}

/// Role of a panel member.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, EnumIter, AsRefStr)]
#[serde(rename_all = "snake_case")]
pub enum PanelRole {
    /// Chair of the panel (Przewodniczący).
    Chair,
    /// Panel member (Członek).
    Member,
    /// Rapporteur preparing the case (Referent).
    Rapporteur,
    /// Other role not explicitly categorised.
    Other,
}

/// Procurement procedure type.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, EnumIter, AsRefStr)]
#[serde(rename_all = "snake_case")]
pub enum ProcedureType {
    /// Open tender (przetarg nieograniczony).
    Open,
    /// Restricted tender (przetarg ograniczony).
    Restricted,
    /// Competitive dialogue.
    CompetitiveDialogue,
    /// Negotiated procedure.
    Negotiated,
    /// Design contest.
    DesignContest,
    /// Any other procedure type.
    Other,
}

/// Statute family.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, EnumIter, AsRefStr)]
#[serde(rename_all = "snake_case")]
pub enum StatuteCode {
    /// Public Procurement Law (Prawo zamówień publicznych).
    Pzp,
    /// Civil Code (Kodeks cywilny).
    Kc,
    /// Code of Civil Procedure (Kodeks postępowania cywilnego).
    Kpc,
    /// Any other statute.
    Other,
}

/// High-level issue taxonomy.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, EnumIter, AsRefStr)]
#[serde(rename_all = "snake_case")]
pub enum IssueKey {
    /// Allegations concerning abnormally low price.
    LowPrice,
    /// Requirements for participation in the procedure.
    ParticipationRequirements,
    /// Description of the subject of the contract.
    SubjectMatter,
    /// Formal defects (e.g., missing documents).
    FormalDefect,
    /// Evaluation criteria and their application.
    EvaluationCriteria,
    /// Exclusion of a contractor.
    Exclusion,
    /// Other issue.
    Other,
}

fn string_enum_schema(
    description: &'static str,
    variants: &[(&'static str, &'static str)],
) -> Schema {
    let mut schema = Map::new();
    schema.insert("type".to_string(), Value::String("string".to_string()));
    schema.insert(
        "enum".to_string(),
        Value::Array(
            variants
                .iter()
                .map(|(value, _)| Value::String((*value).to_string()))
                .collect(),
        ),
    );

    let mut meta_description = description.to_string();
    let variant_notes: Vec<String> = variants
        .iter()
        .filter_map(|(value, note)| {
            if note.is_empty() {
                None
            } else {
                Some(format!("{value}: {note}"))
            }
        })
        .collect();
    if !variant_notes.is_empty() {
        if !meta_description.is_empty() {
            meta_description.push(' ');
        }
        meta_description.push_str("Variants: ");
        meta_description.push_str(&variant_notes.join(" "));
    }

    if !meta_description.is_empty() {
        schema.insert("description".to_string(), Value::String(meta_description));
    }

    Schema::from(schema)
}

fn schema_nullable_procedure_type(generator: &mut SchemaGenerator) -> Schema {
    let base_schema = ProcedureType::json_schema(generator);
    let base_value: Value = base_schema.into();
    let mut object = match base_value {
        Value::Object(map) => map,
        _ => Map::new(),
    };
    object.insert("nullable".to_string(), Value::Bool(true));
    Schema::from(object)
}

impl JsonSchema for DecisionResult {
    fn schema_name() -> Cow<'static, str> {
        Cow::Borrowed("DecisionResult")
    }

    fn json_schema(_gen: &mut SchemaGenerator) -> Schema {
        string_enum_schema(
            "Result of the appeal.",
            &[
                ("dismissed", "Appeal dismissed; zamawiający prevails."),
                ("upheld", "Appeal upheld in full."),
                ("partially_upheld", "Appeal upheld in part."),
                (
                    "discontinued",
                    "Proceedings discontinued (e.g., withdrawal).",
                ),
                ("annulled", "Previous decision annulled."),
                ("unknown", "Fallback when result is not one of the above."),
            ],
        )
    }
}

impl JsonSchema for PanelRole {
    fn schema_name() -> Cow<'static, str> {
        Cow::Borrowed("PanelRole")
    }

    fn json_schema(_gen: &mut SchemaGenerator) -> Schema {
        string_enum_schema(
            "Role of the adjudicating panel member.",
            &[
                ("chair", "Chair of the panel (Przewodniczący)."),
                ("member", "Panel member (Członek)."),
                ("rapporteur", "Rapporteur preparing the case (Referent)."),
                ("other", "Other or unspecified role."),
            ],
        )
    }
}

impl JsonSchema for ProcedureType {
    fn schema_name() -> Cow<'static, str> {
        Cow::Borrowed("ProcedureType")
    }

    fn json_schema(_gen: &mut SchemaGenerator) -> Schema {
        string_enum_schema(
            "Procurement procedure type.",
            &[
                ("open", "Open tender (przetarg nieograniczony)."),
                ("restricted", "Restricted tender (przetarg ograniczony)."),
                ("competitive_dialogue", "Competitive dialogue."),
                ("negotiated", "Negotiated procedure."),
                ("design_contest", "Design contest."),
                ("other", "Any other procedure type."),
            ],
        )
    }
}

impl JsonSchema for StatuteCode {
    fn schema_name() -> Cow<'static, str> {
        Cow::Borrowed("StatuteCode")
    }

    fn json_schema(_gen: &mut SchemaGenerator) -> Schema {
        string_enum_schema(
            "Code of the statute cited in the reasoning.",
            &[
                (
                    "pzp",
                    "Public Procurement Law (Prawo zamówień publicznych).",
                ),
                ("kc", "Civil Code (Kodeks cywilny)."),
                (
                    "kpc",
                    "Code of Civil Procedure (Kodeks postępowania cywilnego).",
                ),
                ("other", "Any other statute."),
            ],
        )
    }
}

impl JsonSchema for IssueKey {
    fn schema_name() -> Cow<'static, str> {
        Cow::Borrowed("IssueKey")
    }

    fn json_schema(_gen: &mut SchemaGenerator) -> Schema {
        string_enum_schema(
            "High-level categorisation of the dispute.",
            &[
                ("low_price", "Allegations concerning abnormally low price."),
                (
                    "participation_requirements",
                    "Requirements for participation in the procedure.",
                ),
                (
                    "subject_matter",
                    "Description of the subject of the contract.",
                ),
                ("formal_defect", "Formal defects (e.g., missing documents)."),
                (
                    "evaluation_criteria",
                    "Evaluation criteria and their application.",
                ),
                ("exclusion", "Exclusion of a contractor."),
                ("other", "Any other issue."),
            ],
        )
    }
}

/// Validation failures aggregated into a single error.
#[derive(Debug, Error)]
#[error("structured decision validation failed: {issues:?}")]
pub struct StructuredValidationError {
    pub issues: Vec<String>,
}

impl StructuredValidationError {
    pub fn with_issue(issue: impl Into<String>) -> Self {
        Self {
            issues: vec![issue.into()],
        }
    }
}

fn validate_date(value: &str) -> Result<(), DateError> {
    if NaiveDate::parse_from_str(value, "%Y-%m-%d").is_ok() {
        return Ok(());
    }
    Err(DateError {
        value: value.to_string(),
    })
}

#[derive(Debug)]
struct DateError {
    value: String,
}

impl Display for DateError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "must be ISO 8601 date (YYYY-MM-DD), got '{}'",
            self.value
        )
    }
}

fn is_valid_currency(value: &str) -> bool {
    const LEN: usize = 3;
    value.len() == LEN && value.chars().all(|ch| ch.is_ascii_uppercase())
}

fn is_valid_cpv(value: &str) -> bool {
    // Accept either plain 8 digits (canonical) or 8 digits followed by a hyphen and a
    // single check digit, which is a common presentation in source documents.
    static CPV_RE: OnceLock<Regex> = OnceLock::new();
    let regex = CPV_RE.get_or_init(|| Regex::new(r"^\d{8}(-\d)?$").expect("cpv regex compiles"));
    regex.is_match(value)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_valid_decision() -> StructuredDecision {
        StructuredDecision {
            identifiers: Identifiers {
                kio_docket: vec!["KIO/UZP 113/08".to_string()],
                saos_id: None,
            },
            decision: DecisionInfo {
                date: "2025-08-31".to_string(),
                result: DecisionResult::Upheld,
                orders: vec!["Nakazuje powtórzyć ocenę ofert.".to_string()],
            },
            panel: vec![PanelMember {
                name: "Jan Kowalski".to_string(),
                role: PanelRole::Chair,
            }],
            parties: Parties {
                contracting_authority: "Komenda Główna Policji".to_string(),
                appellant: "S&T Services Polska Sp. z o.o.".to_string(),
                awardee: Some("ComArch S.A.".to_string()),
                interested_bidders: vec!["Betacom S.A.".to_string()],
            },
            procurement: Procurement {
                subject: Some("System informatyczny".to_string()),
                procedure_type: Some(ProcedureType::Open),
                tender_id: Some("KGP-123/08".to_string()),
                cpv_codes: vec!["72222300".to_string()],
            },
            costs: vec![CostOrder {
                payer: Some("Komenda Główna Policji".to_string()),
                recipient: Some("S&T Services Polska Sp. z o.o.".to_string()),
                amount: 3600.0,
                currency: "PLN".to_string(),
                label: Some("Koszty postępowania".to_string()),
            }],
            statutes_cited: vec![StatuteMention {
                code: StatuteCode::Pzp,
                article: "190".to_string(),
                paragraph: Some("1".to_string()),
                point: Some("2".to_string()),
                quote: None,
            }],
            issues: vec![IssueTag {
                issue_key: IssueKey::EvaluationCriteria,
                confidence: 80,
            }],
            chunks: vec![
                SemanticChunk {
                    position: 1,
                    section: Some("sentencja".to_string()),
                    body: "Opis przebiegu postępowania wraz z uczestnikami.".to_string(),
                },
                SemanticChunk {
                    position: 2,
                    section: Some("uzasadnienie".to_string()),
                    body: "Szczegółowe uzasadnienie z podstawą prawną.".to_string(),
                },
            ],
            summary_short: "Izba uwzględniła odwołanie i nakazała powtórzenie oceny ofert."
                .to_string(),
        }
    }

    #[test]
    fn validates_happy_path() {
        let decision = make_valid_decision();
        assert!(decision.validate().is_ok());
    }

    #[test]
    fn detects_invalid_fields() {
        let mut decision = make_valid_decision();
        decision.decision.date = "31-08-2025".to_string();
        decision.parties.contracting_authority.clear();
        decision.procurement.cpv_codes.push("12A".to_string());
        decision.summary_short.clear();
        decision.costs[0].currency = "pln".to_string();
        decision.chunks[0].body.clear();
        decision.chunks[1].position = 4;
        decision.chunks[0].section = Some("".to_string());

        let error = decision.validate().expect_err("validation must fail");
        assert!(
            error
                .issues
                .iter()
                .any(|issue| issue.contains("decision.date")),
            "{:?}",
            error.issues
        );
        assert!(
            error
                .issues
                .iter()
                .any(|issue| issue.contains("parties.contracting_authority")),
            "{:?}",
            error.issues
        );
        assert!(
            error
                .issues
                .iter()
                .any(|issue| issue.contains("cpv_codes[1]")),
            "{:?}",
            error.issues
        );
        assert!(
            error
                .issues
                .iter()
                .any(|issue| issue.contains("summary_short")),
            "{:?}",
            error.issues
        );
        assert!(
            error.issues.iter().any(|issue| issue.contains("currency")),
            "{:?}",
            error.issues
        );
        assert!(
            error
                .issues
                .iter()
                .any(|issue| issue.contains("chunks[0].body")),
            "{:?}",
            error.issues
        );
        assert!(
            error
                .issues
                .iter()
                .any(|issue| issue.contains("chunks[1].position")),
            "{:?}",
            error.issues
        );
    }

    #[test]
    fn accepts_hyphenated_cpv() {
        let mut decision = make_valid_decision();
        decision.procurement.cpv_codes = vec!["39120000-3".to_string(), "72222300".to_string()];
        assert!(decision.validate().is_ok());
    }

    #[test]
    fn schema_generation_succeeds() {
        let schema = schemars::schema_for!(StructuredDecision);
        let schema_value = serde_json::to_value(schema).expect("schema serializable");
        assert!(schema_value.is_object());
    }
}
