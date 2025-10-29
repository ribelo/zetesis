//! Prompt construction utilities for the structured extraction pipeline.
//!
//! These helpers stay pure and generate the textual instructions and schema
//! payloads needed by the Gemini structured-output endpoint.

use serde_json::Value as JsonValue;

use super::structured::{
    DecisionResult, IssueKey, PanelRole, ProcedureType, StatuteCode, StructuredDecision,
};
use strum::IntoEnumIterator;

/// Complete prompt package for a single document.
#[derive(Debug, Clone)]
pub struct ExtractionPrompt {
    pub system_message: String,
    pub user_message: String,
    pub retry_user_message: String,
    pub schema: JsonValue,
}

/// Build the prompt skeleton for the provided document text.
pub fn build_prompt(document_text: &str) -> ExtractionPrompt {
    let schema = StructuredDecision::schema();
    let schema_pretty =
        serde_json::to_string_pretty(&schema).unwrap_or_else(|_| schema.to_string());

    let system_message = "Jesteś ekspertem prawa zamówień publicznych analizującym orzeczenia Krajowej Izby Odwoławczej.\
        Twoim zadaniem jest zwrócić wyłącznie poprawny JSON opisujący sprawę.\
        Nie dodawaj komentarzy, nie używaj Markdown ani dodatkowego tekstu."
        .to_string();

    let guidelines = build_guidelines();

    let user_intro = format!(
        "Przeanalizuj poniższe orzeczenie KIO i zwróć JSON dokładnie zgodny z podanym schematem. \
        Wypełniaj wszystkie pola. Jeżeli informacja nie występuje, użyj null lub pustej tablicy. \
        Nie opuszczaj wymaganych kluczy.\n\nWytyczne:\n{guidelines}\nSchemat JSON:\n{schema_pretty}\n\nTekst orzeczenia:\n```text\n{document_text}\n```"
    );

    let retry_intro = format!(
        "Poprzednia odpowiedź była niepoprawna (brakowało poprawnego JSON). \
        Spróbuj ponownie i zwróć wyłącznie JSON zgodny ze schematem. \
        Jeśli czegoś nie da się ustalić, ustaw null lub pustą tablicę, ale zachowaj wszystkie klucze.\n\nWytyczne:\n{guidelines}\nSchemat JSON:\n{schema_pretty}\n\nTekst orzeczenia:\n```text\n{document_text}\n```"
    );

    ExtractionPrompt {
        system_message,
        user_message: user_intro,
        retry_user_message: retry_intro,
        schema,
    }
}

fn build_guidelines() -> String {
    let mut items: Vec<String> = Vec::new();
    items.push("- `decision.date` musi mieć format ISO 8601 (YYYY-MM-DD).".to_string());
    items.push(format!(
        "- `decision.result` musi być jednym z: {}.",
        enum_list(result_labels())
    ));
    items.push(
        "- Jeśli w orzeczeniu nie ma postanowień, zwróć pustą tablicę `decision.orders`."
            .to_string(),
    );
    items.push(format!(
        "- `panel.role` musi być jednym z: {}.",
        enum_list(panel_labels())
    ));
    items.push("- W panelu wymień wszystkie osoby znane z tekstu; jeżeli brakuje imienia/nazwiska, przepisz dokładnie to, co występuje.".to_string());
    items.push("- `parties.contracting_authority` to zamawiający, `parties.appellant` to odwołujący. `awardee` to wykonawca, którego oferta została wybrana; jeśli brak, ustaw null.".to_string());
    items.push(format!(
        "- `procurement.procedure_type` użyj jednego z: {}. Jeśli nieznane, ustaw null.",
        enum_list(procedure_labels())
    ));
    items.push("- `procurement.cpv_codes` wypisz wszystkie kody CPV, każdy 8 cyfr; brak oznacza pustą tablicę.".to_string());
    items.push("- `costs[].currency` musi być kodem ISO 4217 (np. PLN, EUR). Kwoty podawaj jako liczby (kropka jako separator dziesiętny).".to_string());
    items.push(format!(
        "- `statutes_cited[].code` wybierz spośród: {}. Artykuły bez znaków dodatkowych.",
        enum_list(statute_labels())
    ));
    items.push(format!(
        "- `issues[].issue_key` wybierz spośród: {}. `confidence` ustaw w skali 0-100, nawet jeśli to szacunek.",
        enum_list(issue_labels())
    ));
    items.push("- `summary_short` przygotuj w języku polskim, w 1-2 zdaniach, streszczając sedno rozstrzygnięcia.".to_string());
    items.push("- Zachowaj strukturę JSON identyczną ze schematem: nawet jeżeli wartość jest nieznana, klucz musi pozostać.".to_string());

    items
        .into_iter()
        .map(|line| format!("  * {line}"))
        .collect::<Vec<_>>()
        .join("\n")
}

fn enum_list(values: Vec<String>) -> String {
    values.join(", ")
}

fn result_labels() -> Vec<String> {
    DecisionResult::iter()
        .map(|variant| variant.as_ref().to_string())
        .collect()
}

fn panel_labels() -> Vec<String> {
    PanelRole::iter()
        .map(|variant| variant.as_ref().to_string())
        .collect()
}

fn procedure_labels() -> Vec<String> {
    ProcedureType::iter()
        .map(|variant| variant.as_ref().to_string())
        .collect()
}

fn statute_labels() -> Vec<String> {
    StatuteCode::iter()
        .map(|variant| variant.as_ref().to_string())
        .collect()
}

fn issue_labels() -> Vec<String> {
    IssueKey::iter()
        .map(|variant| variant.as_ref().to_string())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prompt_contains_schema_and_guidelines() {
        let sample = "Sygn. akt KIO/UZP 113/08 ...";
        let prompt = build_prompt(sample);
        assert!(prompt.user_message.contains("decision.result"));
        assert!(prompt.user_message.contains("\"summary_short\""));
        assert!(prompt.user_message.contains("dismissed"));
        assert!(prompt.schema.is_object());
    }

    #[test]
    fn retry_prompt_is_different() {
        let sample = "Izba postanowiła ...";
        let prompt = build_prompt(sample);
        assert_ne!(prompt.user_message, prompt.retry_user_message);
        assert!(
            prompt
                .retry_user_message
                .contains("Poprzednia odpowiedź była niepoprawna")
        );
    }
}
