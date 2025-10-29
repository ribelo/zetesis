//! Structured representation of KIO decisions extracted from OCR output.
//!
//! These data types model the minimum viable contract we expect from the
//! structured extraction stage. They stay pure, provide JSON schema generation
//! for prompting, and expose validation so downstream code can refuse malformed
//! payloads before writing to storage or search indices.

use std::{
    fmt::{self, Display},
    sync::OnceLock,
};

use chrono::NaiveDate;
use regex::Regex;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use strum::{AsRefStr, EnumIter};
use thiserror::Error;

/// Canonical structured payload for a single KIO decision.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct StructuredDecision {
    pub identifiers: Identifiers,
    pub decision: DecisionInfo,
    pub panel: Vec<PanelMember>,
    pub parties: Parties,
    #[serde(default, skip_serializing_if = "Procurement::is_empty")]
    pub procurement: Procurement,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub costs: Vec<CostOrder>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub statutes_cited: Vec<StatuteMention>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub issues: Vec<IssueTag>,
    #[serde(default)]
    pub summary_short: String,
}

impl StructuredDecision {
    /// Generate a JSON schema describing this payload.
    pub fn schema() -> JsonValue {
        let schema = schemars::schema_for!(StructuredDecision);
        serde_json::to_value(&schema).expect("schema is serializable")
    }

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
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub kio_docket: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub saos_id: Option<String>,
}

/// Core decision outcome metadata.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct DecisionInfo {
    pub date: String,
    pub result: DecisionResult,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub orders: Vec<String>,
}

/// Member of the adjudicating panel.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct PanelMember {
    pub name: String,
    pub role: PanelRole,
}

/// Parties involved in the proceedings.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct Parties {
    pub contracting_authority: String,
    pub appellant: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub awardee: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub interested_bidders: Vec<String>,
}

/// Procurement metadata associated with the decision.
#[derive(Debug, Clone, Default, Serialize, Deserialize, JsonSchema)]
pub struct Procurement {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub subject: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub procedure_type: Option<ProcedureType>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tender_id: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
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
                issues.push(format!("cpv_codes[{idx}] must be 8 digits, got '{cpv}'"));
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
    pub payer: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub recipient: Option<String>,
    pub amount: f64,
    pub currency: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

/// Mention of a statute in the reasoning.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct StatuteMention {
    pub code: StatuteCode,
    pub article: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub paragraph: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub point: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quote: Option<String>,
}

/// Topical categorisation of the dispute.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub struct IssueTag {
    pub issue_key: IssueKey,
    #[serde(default)]
    pub confidence: u8,
}

/// Result of the decision regarding the appeal.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, EnumIter, AsRefStr,
)]
#[serde(rename_all = "snake_case")]
pub enum DecisionResult {
    Dismissed,
    Upheld,
    PartiallyUpheld,
    Discontinued,
    Annulled,
    #[strum(disabled)]
    #[serde(other)]
    Unknown,
}

/// Role of a panel member.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, EnumIter, AsRefStr,
)]
#[serde(rename_all = "snake_case")]
pub enum PanelRole {
    Chair,
    Member,
    Rapporteur,
    Other,
}

/// Procurement procedure type.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, EnumIter, AsRefStr,
)]
#[serde(rename_all = "snake_case")]
pub enum ProcedureType {
    Open,
    Restricted,
    CompetitiveDialogue,
    Negotiated,
    DesignContest,
    Other,
}

/// Statute family.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, EnumIter, AsRefStr,
)]
#[serde(rename_all = "snake_case")]
pub enum StatuteCode {
    Pzp,
    Kc,
    Kpc,
    Other,
}

/// High-level issue taxonomy.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema, EnumIter, AsRefStr,
)]
#[serde(rename_all = "snake_case")]
pub enum IssueKey {
    LowPrice,
    ParticipationRequirements,
    SubjectMatter,
    FormalDefect,
    EvaluationCriteria,
    Exclusion,
    Other,
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
    static CPV_RE: OnceLock<Regex> = OnceLock::new();
    let regex = CPV_RE.get_or_init(|| Regex::new(r"^\d{8}$").expect("cpv regex compiles"));
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
    }

    #[test]
    fn schema_generation_succeeds() {
        let schema = StructuredDecision::schema();
        assert!(schema.is_object());
    }
}
