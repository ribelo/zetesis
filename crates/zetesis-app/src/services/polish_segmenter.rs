use std::collections::HashSet;
use std::ops::Range;
use std::sync::{Arc, LazyLock};

use bon::Builder;
use icu_segmenter::{
    SentenceSegmenter, SentenceSegmenterBorrowed, options::SentenceBreakInvariantOptions,
};

static SENTENCE_SEGMENTER: LazyLock<SentenceSegmenterBorrowed<'static>> =
    LazyLock::new(|| SentenceSegmenter::new(SentenceBreakInvariantOptions::default()));

static BASE_ABBREVIATIONS: &[&str] = &[
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

static BASE_ABBREVIATIONS_SET: LazyLock<HashSet<String>> = LazyLock::new(|| {
    let mut set = HashSet::new();
    for &abbr in BASE_ABBREVIATIONS {
        insert_abbreviation(&mut set, abbr);
    }
    set
});

fn insert_abbreviation(set: &mut HashSet<String>, abbr: &str) {
    if abbr.is_empty() {
        return;
    }

    let lower = abbr.to_lowercase();
    set.insert(lower.clone());
    set.insert(abbr.to_string());

    if let Some(stripped) = abbr.strip_suffix('.') {
        set.insert(stripped.to_string());
        set.insert(stripped.to_lowercase());
    }
}

#[derive(Debug, Clone, Builder)]
pub struct SegmenterOptions {
    #[builder(default = true)]
    pub stitch_abbreviations: bool,
    #[builder(default = true)]
    pub stitch_ordinals: bool,
    #[builder(into)]
    pub extra_abbreviations: Option<Arc<[String]>>,
}

impl Default for SegmenterOptions {
    fn default() -> Self {
        Self {
            stitch_abbreviations: true,
            stitch_ordinals: true,
            extra_abbreviations: None,
        }
    }
}

/// Sentence segmentation tuned for Polish legal documents.
pub struct PolishSentenceSegmenter;

impl PolishSentenceSegmenter {
    /// Returns trimmed sentence ranges for the provided text.
    pub fn ranges(text: &str) -> Vec<Range<usize>> {
        Self::ranges_with(text, &SegmenterOptions::default())
    }

    /// Returns trimmed sentence ranges with explicit configuration.
    pub fn ranges_with(text: &str, options: &SegmenterOptions) -> Vec<Range<usize>> {
        if text.is_empty() {
            return Vec::new();
        }

        let mut boundaries: Vec<usize> = SENTENCE_SEGMENTER.segment_str(text).collect();

        if boundaries.first().copied().unwrap_or_default() != 0 {
            boundaries.insert(0, 0);
        }
        if boundaries.last().copied().unwrap_or_default() != text.len() {
            boundaries.push(text.len());
        }

        let mut ranges = Vec::with_capacity(boundaries.len().saturating_sub(1));
        for window in boundaries.windows(2) {
            let range = window[0]..window[1];
            if let Some(trimmed) = trim_range(text, range) {
                ranges.push(trimmed);
            }
        }

        if ranges.is_empty() {
            return ranges;
        }

        let extras = options
            .extra_abbreviations
            .as_ref()
            .map(|entries| build_extra_set(entries));

        let mut merged = Vec::with_capacity(ranges.len());
        let mut iter = ranges.into_iter().peekable();

        while let Some(mut current) = iter.next() {
            while let Some(next) = iter.peek() {
                if should_merge(text, &current, next, options, extras.as_ref()) {
                    let next = iter.next().expect("peeked element must exist");
                    current = current.start..next.end;
                } else {
                    break;
                }
            }
            merged.push(current);
        }

        merged
            .into_iter()
            .flat_map(|range| split_enumerations(text, range))
            .collect()
    }

    /// Returns trimmed sentence strings for the provided text.
    pub fn split(text: &str) -> Vec<String> {
        Self::split_with(text, &SegmenterOptions::default())
    }

    /// Returns trimmed sentence strings with explicit configuration.
    pub fn split_with(text: &str, options: &SegmenterOptions) -> Vec<String> {
        Self::ranges_with(text, options)
            .into_iter()
            .map(|range| text[range].to_string())
            .collect()
    }
}

pub trait PolishSentenceSplit {
    fn split_polish_sentences(&self) -> Vec<String>;
}

impl<T: AsRef<str>> PolishSentenceSplit for T {
    fn split_polish_sentences(&self) -> Vec<String> {
        PolishSentenceSegmenter::split(self.as_ref())
    }
}

fn should_merge(
    text: &str,
    current: &Range<usize>,
    next: &Range<usize>,
    options: &SegmenterOptions,
    extra: Option<&HashSet<String>>,
) -> bool {
    if !options.stitch_abbreviations && !options.stitch_ordinals {
        return false;
    }

    let tail_tokens = tail_tokens(text, current, 3);
    if tail_tokens.is_empty() {
        return false;
    }
    let last_token_raw = *tail_tokens.last().unwrap();
    let last_token = trim_token(last_token_raw);

    let head_token = first_token(text, next);
    let head_trimmed = head_token.map(trim_token);

    if options.stitch_abbreviations && tokens_match_abbreviation(&tail_tokens, extra) {
        return true;
    }

    if options.stitch_abbreviations {
        if let (Some(last), Some(head)) = (
            (!last_token.is_empty()).then_some(last_token),
            head_trimmed.filter(|t| !t.is_empty()),
        ) {
            let combined = format!("{last} {head}");
            if phrase_is_abbreviation(&combined, extra) {
                return true;
            }
        }
    }

    if options.stitch_ordinals && token_is_ordinal(last_token) {
        return true;
    }

    if options.stitch_abbreviations
        && last_token_raw.ends_with(':')
        && head_token.map_or(false, |token| {
            token
                .chars()
                .next()
                .map_or(false, |c| c.is_ascii_lowercase())
        })
    {
        // Headings often use "Rozdz. 8." / "Sekcja:" patterns followed by lowercase continuation.
        return true;
    }

    false
}

fn build_extra_set(entries: &Arc<[String]>) -> HashSet<String> {
    let mut set = HashSet::with_capacity(entries.len() * 2);
    for entry in entries.iter() {
        let trimmed = entry.trim();
        if trimmed.is_empty() {
            continue;
        }
        insert_abbreviation(&mut set, trimmed);
    }
    set
}

fn token_is_abbreviation(token: &str, extra: Option<&HashSet<String>>) -> bool {
    let trimmed =
        token.trim_matches(|c: char| matches!(c, ',' | ';' | ':' | '"' | '\'' | ')' | '('));
    if trimmed.is_empty() {
        return false;
    }

    if is_initial(trimmed) {
        return true;
    }

    if lookup_abbreviation(trimmed, extra) {
        return true;
    }

    if let Some(stripped) = trimmed.strip_suffix('.') {
        if lookup_abbreviation(stripped, extra) {
            return true;
        }
    }

    false
}

fn lookup_abbreviation(candidate: &str, extra: Option<&HashSet<String>>) -> bool {
    let lower = candidate.to_lowercase();
    BASE_ABBREVIATIONS_SET.contains(candidate)
        || BASE_ABBREVIATIONS_SET.contains(&lower)
        || extra.map_or(false, |set| set.contains(candidate) || set.contains(&lower))
}

fn token_is_ordinal(token: &str) -> bool {
    let trimmed =
        token.trim_matches(|c: char| matches!(c, '.' | ')' | '(' | '"' | '\'' | ',' | ';'));
    if trimmed.is_empty() {
        return false;
    }

    if trimmed.chars().all(|c| c.is_ascii_digit()) {
        return true;
    }

    let upper = trimmed.to_ascii_uppercase();
    !upper.is_empty()
        && upper.len() <= 8
        && upper
            .chars()
            .all(|c| matches!(c, 'I' | 'V' | 'X' | 'L' | 'C' | 'D' | 'M'))
}

fn trim_range(text: &str, range: Range<usize>) -> Option<Range<usize>> {
    if range.start >= range.end || range.end > text.len() {
        return None;
    }
    let slice = &text[range.clone()];
    let trimmed_start = slice.trim_start();
    if trimmed_start.is_empty() {
        return None;
    }
    let leading = slice.len() - trimmed_start.len();
    let trimmed = trimmed_start.trim_end();
    let trailing = trimmed_start.len() - trimmed.len();

    let start = range.start + leading;
    let end = range.end - trailing;

    if start < end { Some(start..end) } else { None }
}

fn first_token<'a>(text: &'a str, range: &Range<usize>) -> Option<&'a str> {
    text[range.clone()].trim_start().split_whitespace().next()
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

fn trim_token(token: &str) -> &str {
    token.trim_matches(|c: char| matches!(c, ',' | ';' | ':' | '"' | '\'' | ')' | '('))
}

fn tail_tokens<'a>(text: &'a str, range: &Range<usize>, limit: usize) -> Vec<&'a str> {
    if limit == 0 {
        return Vec::new();
    }

    let mut tokens = Vec::new();
    for token in text[range.clone()].trim_end().split_whitespace().rev() {
        tokens.push(token);
        if tokens.len() == limit {
            break;
        }
    }
    tokens.reverse();
    tokens
}

fn tokens_match_abbreviation(tokens: &[&str], extra: Option<&HashSet<String>>) -> bool {
    let trimmed: Vec<&str> = tokens
        .iter()
        .map(|token| trim_token(token))
        .filter(|token| !token.is_empty())
        .collect();

    if let Some(last) = trimmed.last() {
        if token_is_abbreviation(last, extra) {
            return true;
        }
    }

    for start in 0..trimmed.len() {
        let candidate = trimmed[start..].join(" ");
        if phrase_is_abbreviation(&candidate, extra) {
            return true;
        }
    }

    false
}

fn phrase_is_abbreviation(candidate: &str, extra: Option<&HashSet<String>>) -> bool {
    let trimmed =
        candidate.trim_matches(|c: char| matches!(c, ',' | ';' | ':' | '"' | '\'' | ')' | '('));
    if trimmed.is_empty() {
        return false;
    }

    if lookup_abbreviation(trimmed, extra) {
        return true;
    }

    if let Some(stripped) = trimmed.strip_suffix('.') {
        if lookup_abbreviation(stripped, extra) {
            return true;
        }
    }

    let tokens: Vec<&str> = trimmed.split_whitespace().collect();
    if tokens.is_empty() {
        return false;
    }

    if tokens.iter().all(|token| {
        let stripped =
            token.trim_matches(|c: char| matches!(c, ',' | ';' | ':' | '"' | '\'' | ')' | '('));
        is_initial(stripped)
    }) {
        return true;
    }

    false
}

fn split_enumerations(text: &str, range: Range<usize>) -> Vec<Range<usize>> {
    let slice = &text[range.clone()];
    if slice.is_empty() {
        return vec![range];
    }

    let mut result = Vec::new();
    let mut current_start = range.start;
    let chars: Vec<(usize, char)> = slice.char_indices().collect();
    let mut idx = 0;
    let mut allow_follow_on = false;

    while idx < chars.len() {
        let (offset, _) = chars[idx];
        if offset == 0 {
            if let Some(marker_len) = enumeration_marker(slice, offset) {
                allow_follow_on = true;
                let target = marker_len;
                while idx < chars.len() && chars[idx].0 < target {
                    idx += 1;
                }
                continue;
            }
            idx += 1;
            continue;
        }

        if allow_follow_on || preceding_allows_marker(slice, offset) {
            if let Some(marker_len) = enumeration_marker(slice, offset) {
                let boundary = range.start + offset;
                if boundary > current_start {
                    if let Some(trimmed) = trim_range(text, current_start..boundary) {
                        result.push(trimmed);
                    }
                    current_start = boundary;
                }

                allow_follow_on = true;
                let target = offset + marker_len;
                while idx < chars.len() && chars[idx].0 < target {
                    idx += 1;
                }
                continue;
            }
        }

        idx += 1;
    }

    if let Some(trimmed) = trim_range(text, current_start..range.end) {
        result.push(trimmed);
    }

    if result.is_empty() {
        if let Some(trimmed) = trim_range(text, range) {
            result.push(trimmed);
        }
    }

    result
}

fn preceding_allows_marker(slice: &str, offset: usize) -> bool {
    if offset == 0 {
        return false;
    }

    slice[..offset]
        .chars()
        .rev()
        .find(|ch| !ch.is_whitespace())
        .map_or(true, |ch| {
            matches!(
                ch,
                ':' | ';' | '(' | ')' | '[' | ']' | '-' | '–' | '—' | '\n'
            )
        })
}

fn enumeration_marker(slice: &str, offset: usize) -> Option<usize> {
    let rest = &slice[offset..];
    let chars: Vec<(usize, char)> = rest.char_indices().collect();
    if chars.is_empty() {
        return None;
    }

    let first_char = chars[0].1;

    if first_char.is_ascii_digit() {
        let mut idx = 0;
        let mut digits = 0;
        while idx < chars.len() && chars[idx].1.is_ascii_digit() && digits < 3 {
            digits += 1;
            idx += 1;
        }

        if digits == 0 {
            return None;
        }

        while idx < chars.len() && chars[idx].1.is_whitespace() {
            idx += 1;
        }

        if idx >= chars.len() {
            return None;
        }

        let punct = chars[idx].1;
        if punct != '.' && punct != ')' {
            return None;
        }

        let marker_end = chars[idx].0 + punct.len_utf8();
        idx += 1;

        if idx >= chars.len() || !chars[idx].1.is_whitespace() {
            return None;
        }

        Some(marker_end)
    } else if first_char.is_ascii_uppercase() {
        if chars
            .get(1)
            .map_or(false, |(_, ch)| ch.is_ascii_lowercase())
        {
            return None;
        }

        let mut idx = 1;
        while idx < chars.len() && chars[idx].1.is_whitespace() {
            idx += 1;
        }

        if idx >= chars.len() {
            return None;
        }

        let punct = chars[idx].1;
        if punct != '.' && punct != ')' {
            return None;
        }

        let marker_end = chars[idx].0 + punct.len_utf8();
        idx += 1;

        if idx >= chars.len() || !chars[idx].1.is_whitespace() {
            return None;
        }

        Some(marker_end)
    } else {
        None
    }
}
