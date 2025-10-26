use bon::Builder;
use once_cell::sync::Lazy;
use regex::Regex;
use unicode_normalization::UnicodeNormalization;

#[derive(Debug, Clone, Builder)]
pub struct CleanupOptions {
    #[builder(default = true)]
    pub unicode_normalize: bool,
    #[builder(default = true)]
    pub strip_page_markers: bool,
    #[builder(default = true)]
    pub join_hyphenated_breaks: bool,
    #[builder(default = true)]
    pub collapse_whitespace: bool,
}

impl Default for CleanupOptions {
    fn default() -> Self {
        Self {
            unicode_normalize: true,
            strip_page_markers: true,
            join_hyphenated_breaks: true,
            collapse_whitespace: true,
        }
    }
}

/// Normalizes raw document text extracted from PDFs before sentence splitting.
pub fn cleanup_text(text: &str) -> String {
    cleanup_text_with_options(text, &CleanupOptions::default())
}

/// Cleans up text according to the provided [`CleanupOptions`].
pub fn cleanup_text_with_options(text: &str, options: &CleanupOptions) -> String {
    let mut cleaned: String = text
        .chars()
        .filter(|c| !c.is_control() || matches!(c, '\t' | '\n' | '\r'))
        .collect();

    if options.unicode_normalize {
        cleaned = cleaned.nfkc().collect();
    }
    if options.strip_page_markers {
        cleaned = remove_page_numbers(&cleaned);
    }
    if options.join_hyphenated_breaks {
        cleaned = join_hyphenated_words(&cleaned);
    }
    cleaned = insert_missing_spaces(&cleaned);
    if options.collapse_whitespace {
        cleaned = collapse_whitespace(&cleaned);
    }

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

fn insert_missing_spaces(input: &str) -> String {
    static RE: Lazy<Regex> = Lazy::new(|| {
        Regex::new(r"(?u)(\p{Ll})([A-ZŁŚĆŻŹÓĘĄŃ])([ąćęłńóśżź])").unwrap()
    });
    RE.replace_all(input, "$1 $2$3").into_owned()
}

fn collapse_whitespace(input: &str) -> String {
    let mut collapsed = Vec::with_capacity(input.len() / 4);
    for fragment in input.split_whitespace() {
        if !fragment.is_empty() {
            collapsed.push(fragment);
        }
    }
    collapsed.join(" ")
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

    #[test]
    fn joins_hyphenated_breaks() {
        let input = "charak-\nter";
        assert_eq!(cleanup_text(input), "charakter");
    }
}
