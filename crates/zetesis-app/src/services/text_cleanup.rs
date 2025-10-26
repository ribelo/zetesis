use once_cell::sync::Lazy;
use regex::Regex;
use unicode_normalization::UnicodeNormalization;

/// Normalizes raw document text extracted from PDFs before sentence splitting.
pub fn cleanup_text(text: &str) -> String {
    let mut cleaned = text
        .chars()
        .filter(|c| !c.is_control() || matches!(c, '\t' | '\n' | '\r'))
        .collect::<String>();

    cleaned = cleaned.nfkc().collect::<String>();
    cleaned = remove_page_numbers(&cleaned);
    cleaned = join_hyphenated_words(&cleaned);
    cleaned = trim_apostrophe_spacing(&cleaned);
    cleaned = normalize_money_groups(&cleaned);

    cleaned = cleaned.replace('\n', " ");
    cleaned = collapse_whitespace(&cleaned);

    cleaned.trim().to_string()
}

fn remove_page_numbers(input: &str) -> String {
    static RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\n\s*\d+\s*(?:/\s*\d+)?\s*\n").unwrap());
    RE.replace_all(input, "\n").into_owned()
}

fn join_hyphenated_words(input: &str) -> String {
    static RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"(?m)(\w+)-\s*\n\s*(\w+)").unwrap());
    RE.replace_all(input, "$1$2").into_owned()
}

fn trim_apostrophe_spacing(input: &str) -> String {
    static RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\s+'(\w)").unwrap());
    RE.replace_all(input, "'$1").into_owned()
}

fn normalize_money_groups(input: &str) -> String {
    static RE: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"(\d{1,3})\s+(\d{3}),\s*(\d{2})\s*(zł)").unwrap());
    RE.replace_all(input, "$1$2,$3 $4").into_owned()
}

fn collapse_whitespace(input: &str) -> String {
    input
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
        .replace('"', "\"")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trims_multiple_spaces() {
        let input = "Tak  informacja  odwołującego  różniła  się.";
        assert_eq!(
            cleanup_text(input),
            "Tak informacja odwołującego różniła się."
        );
    }
}
