use std::collections::HashSet;
use std::sync::LazyLock;

static ABBREVIATIONS: &[&str] = &[
    "art.",
    "ust.",
    "pkt.",
    "lit.",
    "poz.",
    "t.j.",
    "tj.",
    "r.",
    "nr",
    "z późn. zm.",
    "ze zm.",
    "zw.",
    "sp.",
    "z o.o.",
    "sygn.",
    "dz. u.",
    "zz.",
    "k.c.",
    "a.",
    "b.",
    "c.",
    "d.",
    "np.",
    "wyd.",
    "ul.",
    "pl.",
    "pzp.",
    "pn.",
    "dot.",
    "ww.",
    "m.in.",
    "itp.",
    "etc.",
    "i.e.",
    "s.m.",
    "św.m.",
    "p.m.",
    "j.m.",
    "p.n.",
    "p.w.",
    "tzn.",
    "tzw.",
    "ok.",
    "śr.",
    "min.",
    "max.",
    "maks.",
    "tys.",
    "mln",
    "mld",
    "ha",
    "kg",
    "g",
    "mg",
    "l",
    "ml",
    "cm",
    "mm",
    "m",
    "km",
    "szt.",
    "godz.",
    "min",
    "sek.",
    "temp.",
    "wg",
    "ws.",
    "zob.",
    "por.",
    "ryc.",
    "rys.",
    "tab.",
    "fot.",
    "str.",
    "al.",
    "rozdz.",
];

static ABBREVIATIONS_SET: LazyLock<HashSet<String>> = LazyLock::new(|| {
    let mut set = HashSet::new();
    for &abbr in ABBREVIATIONS {
        set.insert(abbr.to_string());
        set.insert(abbr.to_lowercase());
        let trimmed = abbr.trim_end_matches('.');
        if trimmed != abbr {
            set.insert(trimmed.to_string());
            set.insert(trimmed.to_lowercase());
        }
    }
    set
});

static PAIRED_DELIMITERS: &[(&str, &str)] = &[
    ("<", ">"),
    ("[", "]"),
    ("(", ")"),
    ("{", "}"),
    (r#"""#, r#"""#),
    ("'", "'"),
    ("\"", "\""),
];

/// Segmenter tuned for Polish legal text with expanded abbreviation support.
pub struct PolishSentenceSegmenter;

impl PolishSentenceSegmenter {
    pub fn split(text: &str) -> Vec<String> {
        SentenceParser::new(text).collect()
    }
}

fn sentence_tail_is_abbreviation(sentence: &str) -> bool {
    let trimmed = sentence.trim_end();
    let Some(last) = trimmed.split_whitespace().last() else {
        return false;
    };
    if is_initial(last) {
        return true;
    }
    is_abbreviation_like(last)
}

fn sentence_is_ordinal_marker(sentence: &str) -> bool {
    let trimmed = sentence.trim();
    if trimmed.is_empty() {
        return false;
    }
    let mut parts: Vec<&str> = trimmed.split_whitespace().collect();
    if parts.is_empty() {
        return false;
    }

    let last = parts.pop().unwrap();
    let digits = last.trim_end_matches('.');
    if digits.is_empty() {
        return true;
    }
    if !digits.chars().all(|c| c.is_ascii_digit()) {
        return false;
    }

    parts.into_iter().all(is_abbreviation_like)
}

fn is_abbreviation_like(word: &str) -> bool {
    let trimmed = word.trim_matches(|c: char| matches!(c, '.' | ',' | ';' | ':' | '"' | '\''));
    if trimmed.is_empty() {
        return true;
    }

    if ABBREVIATIONS_SET.contains(trimmed) {
        return true;
    }
    let lower = trimmed.to_lowercase();
    if ABBREVIATIONS_SET.contains(lower.as_str()) {
        return true;
    }

    let mut dotted = trimmed.to_string();
    dotted.push('.');
    if ABBREVIATIONS_SET.contains(dotted.as_str()) {
        return true;
    }
    let dotted_lower = dotted.to_lowercase();
    ABBREVIATIONS_SET.contains(dotted_lower.as_str())
}

fn is_initial(fragment: &str) -> bool {
    let trimmed = fragment.trim();
    trimmed.len() == 2
        && trimmed
            .chars()
            .next()
            .map_or(false, |c| c.is_ascii_uppercase())
        && trimmed.ends_with('.')
}

pub trait PolishSentenceSplit {
    fn split_polish_sentences(&self) -> Vec<String>;
}

impl<T: AsRef<str>> PolishSentenceSplit for T {
    fn split_polish_sentences(&self) -> Vec<String> {
        PolishSentenceSegmenter::split(self.as_ref())
    }
}

#[derive(Debug)]
pub struct SentenceParser<'a> {
    input: &'a str,
    position: usize,
}

impl<'a> SentenceParser<'a> {
    pub fn new(input: &'a str) -> Self {
        Self { input, position: 0 }
    }

    fn is_end(&self) -> bool {
        self.position >= self.input.len()
    }

    fn peek_chars(&self, n: usize) -> &str {
        let remaining = &self.input[self.position..];
        let mut end = 0;
        let mut char_count = 0;

        for ch in remaining.chars() {
            if char_count >= n {
                break;
            }
            end += ch.len_utf8();
            char_count += 1;
        }

        &remaining[..end]
    }

    fn consume_chars(&mut self, n: usize) -> Option<&'a str> {
        let start = self.position;
        let remaining = &self.input[self.position..];

        let mut end = 0;
        let mut char_count = 0;

        for ch in remaining.chars() {
            if char_count >= n {
                break;
            }
            end += ch.len_utf8();
            char_count += 1;
        }

        if end > 0 {
            self.position += end;
            Some(&self.input[start..start + end])
        } else {
            None
        }
    }

    fn maybe_consume_abbreviation(&mut self) -> Option<&'a str> {
        const COMMON_LENGTHS: &[usize] = &[3, 4, 2, 5, 6, 7, 8, 9, 10, 11, 12];
        let prev_char = if self.position == 0 {
            None
        } else {
            self.input[..self.position].chars().rev().next()
        };

        for &len in COMMON_LENGTHS {
            let ahead = self.peek_chars(len);
            if ahead.len() < len {
                continue;
            }

            let has_word_prefix = prev_char.map_or(false, |c| c.is_alphabetic());
            if !has_word_prefix && ABBREVIATIONS_SET.contains(ahead) {
                return self.consume_chars(len);
            }
        }
        None
    }

    fn maybe_consume_digits(&mut self) -> Option<&'a str> {
        let start = self.position;
        let remaining = &self.input[self.position..];

        if !remaining
            .chars()
            .next()
            .map_or(false, |c| c.is_ascii_digit())
        {
            return None;
        }

        let mut end = 0;
        let mut last_was_dot = false;

        for ch in remaining.chars() {
            match ch {
                '0'..='9' => {
                    end += ch.len_utf8();
                    last_was_dot = false;
                }
                '.' if !last_was_dot => {
                    end += ch.len_utf8();
                    last_was_dot = true;
                }
                _ => break,
            }
        }

        if last_was_dot && end > 1 {
            end -= 1;
        }

        if end > 0 {
            self.position += end;
            Some(&self.input[start..start + end])
        } else {
            None
        }
    }

    fn maybe_consume_nested_delimited(&mut self) -> Option<&'a str> {
        let start_pos = self.position;
        let remaining = &self.input[self.position..];

        for (open, close) in PAIRED_DELIMITERS {
            if !remaining.starts_with(open) {
                continue;
            }

            if open.len() == 1 && close.len() == 1 {
                let open_char = open.chars().next().unwrap();
                let close_char = close.chars().next().unwrap();

                let mut depth = 0;
                let mut end = 0;
                let mut chars = remaining.chars();

                while let Some(ch) = chars.next() {
                    if ch == open_char {
                        depth += 1;
                    } else if ch == close_char {
                        depth -= 1;
                        if depth == 0 {
                            end += ch.len_utf8();
                            self.position += end;
                            return Some(&self.input[start_pos..self.position]);
                        }
                    }

                    end += ch.len_utf8();

                    const MAX_DELIMITER_SEARCH: usize = 1000;
                    if end > MAX_DELIMITER_SEARCH {
                        break;
                    }
                }
            }
        }
        None
    }

    pub fn parse_sentence(&mut self) -> Option<String> {
        if self.is_end() {
            return None;
        }

        let mut sentence = String::new();

        while !self.is_end() {
            if let Some(abbrev) = self.maybe_consume_abbreviation() {
                sentence.push_str(abbrev);
                continue;
            }

            if let Some(digits) = self.maybe_consume_digits() {
                sentence.push_str(digits);
                continue;
            }

            if let Some(delimited) = self.maybe_consume_nested_delimited() {
                sentence.push_str(delimited);
                continue;
            }

            let remaining = &self.input[self.position..];
            if let Some(ch) = remaining.chars().next() {
                sentence.push(ch);
                self.position += ch.len_utf8();

                match ch {
                    '.' | '!' | '?' => {
                        if sentence_tail_is_abbreviation(&sentence) {
                            continue;
                        }

                        if sentence_is_ordinal_marker(&sentence) {
                            continue;
                        }

                        const BOUNDARY_PUNCTUATION: &[char] = &['"', '\'', ')', ']', '}'];
                        let peek_slice = &self.input[self.position..];
                        let next_non_space =
                            peek_slice.chars().skip_while(|c| c.is_whitespace()).next();

                        let ends_input = next_non_space.is_none();
                        let should_break = next_non_space
                            .map(|next| {
                                next.is_uppercase()
                                    || next.is_ascii_digit()
                                    || BOUNDARY_PUNCTUATION.contains(&next)
                            })
                            .unwrap_or(ends_input);

                        if should_break {
                            break;
                        }
                    }
                    _ => {}
                }
            } else {
                break;
            }
        }

        if sentence.is_empty() {
            None
        } else {
            Some(sentence)
        }
    }
}

impl<'a> Iterator for SentenceParser<'a> {
    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {
        self.parse_sentence()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::services::cleanup_text;
    use serde::Deserialize;
    use std::fs;
    use std::path::Path;

    #[derive(Debug, Deserialize)]
    struct Expectation {
        index: usize,
        #[serde(default)]
        contains: Option<String>,
        #[serde(default)]
        starts_with: Option<String>,
        #[serde(default)]
        equals_normalized: Option<String>,
    }

    #[derive(Debug, Deserialize)]
    struct SegmentCase {
        label: String,
        text: String,
        expected_sentences: usize,
        #[serde(default)]
        expectations: Vec<Expectation>,
    }

    #[test]
    fn regressions_from_fixture() {
        let cases_path =
            Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/polish_segmenter_cases.yml");
        let yaml = fs::read_to_string(&cases_path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", cases_path.display()));

        let cases: Vec<SegmentCase> = serde_yaml::from_str(&yaml)
            .unwrap_or_else(|err| panic!("failed to parse {}: {err}", cases_path.display()));

        for case in cases {
            let cleaned = cleanup_text(&case.text);
            let sentences = PolishSentenceSegmenter::split(&cleaned)
                .into_iter()
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect::<Vec<_>>();
            assert_eq!(
                sentences.len(),
                case.expected_sentences,
                "case `{}` expected {} sentences, got {}",
                case.label,
                case.expected_sentences,
                sentences.len()
            );

            for expectation in case.expectations {
                assert!(
                    expectation.index < sentences.len(),
                    "case `{}` expectation index {} out of bounds",
                    case.label,
                    expectation.index
                );
                let sentence = &sentences[expectation.index];

                if let Some(needle) = expectation.contains.as_ref() {
                    assert!(
                        sentence.contains(needle),
                        "case `{}` sentence {} should contain `{}` but was `{}`",
                        case.label,
                        expectation.index,
                        needle,
                        sentence
                    );
                }

                if let Some(prefix) = expectation.starts_with.as_ref() {
                    assert!(
                        sentence.trim_start().starts_with(prefix),
                        "case `{}` sentence {} should start with `{}` but was `{}`",
                        case.label,
                        expectation.index,
                        prefix,
                        sentence
                    );
                }

                if let Some(expected) = expectation.equals_normalized.as_ref() {
                    let normalized = sentence.split_whitespace().collect::<Vec<_>>().join(" ");
                    assert_eq!(
                        normalized, *expected,
                        "case `{}` sentence {} normalized mismatch",
                        case.label, expectation.index
                    );
                }
            }
        }
    }
}
