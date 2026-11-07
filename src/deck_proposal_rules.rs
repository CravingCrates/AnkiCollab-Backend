use crate::database::SharedConn;
use regex::{Regex, RegexBuilder};
use serde::{Deserialize, Serialize};
use tokio_postgres::GenericClient;

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct DeckProposalRules {
    pub deck: i64,
    // HTML Sanitization
    pub strip_colors: bool,
    pub strip_span: bool,
    pub strip_inline_styles: bool,
    pub strip_font_tags: bool,
    pub prevent_center_on_back: bool,
    pub trim_trailing_breaks: bool,
    pub normalize_spaces: bool,
    pub auto_repair_cloze: bool,
    pub strip_empty_elements: bool,

    // Tag Lifecycle
    pub auto_tag_new_cards: bool,
    pub new_card_tag_pattern: String,
    pub auto_tag_updated_cards: bool,
    pub updated_card_tag_pattern: String,
    pub prune_old_update_tags: bool,
    pub old_update_tag_pattern: String,
    pub forbidden_tags: String,
    pub normalize_tag_casing: String,
    pub protected_tag_namespaces: String,

    // Media Optimization
    pub enable_media_compression: bool,
    pub max_image_dimension: i32,
    pub image_quality: i32,
    pub auto_convert_heavy_images: bool,
    pub strip_exif_metadata: bool,
    pub max_media_file_size_mb: i32,

    // PR Policies
    pub require_rationale: bool,
    pub disallow_deletion_suggestions: bool,
    pub max_cards_per_proposal: i32,

    // Custom Rules Injection (.config format)
    pub custom_html_rules: String,
    pub custom_tag_rules: String,
}

impl Default for DeckProposalRules {
    fn default() -> Self {
        Self {
            deck: 0,
            strip_colors: false,
            strip_span: false,
            strip_inline_styles: false,
            strip_font_tags: false,
            prevent_center_on_back: false,
            trim_trailing_breaks: false,
            normalize_spaces: false,
            auto_repair_cloze: false,
            strip_empty_elements: false,

            auto_tag_new_cards: false,
            new_card_tag_pattern: "".to_string(),
            auto_tag_updated_cards: false,
            updated_card_tag_pattern: "".to_string(),
            prune_old_update_tags: false,
            old_update_tag_pattern: "".to_string(),
            forbidden_tags: "".to_string(),
            normalize_tag_casing: "none".to_string(),
            protected_tag_namespaces: "".to_string(),

            enable_media_compression: false,
            max_image_dimension: 0,
            image_quality: 0,
            auto_convert_heavy_images: false,
            strip_exif_metadata: false,
            max_media_file_size_mb: 0,

            require_rationale: false,
            disallow_deletion_suggestions: false,
            max_cards_per_proposal: 0,

            custom_html_rules: "".to_string(),
            custom_tag_rules: "".to_string(),
        }
    }
}

/// Fetch deck proposal rules from the database using any client/transaction (walking up parent decks if `deck_id` is a subdeck) or return default.
pub async fn get_rules_with_client<C: GenericClient>(
    client: &C,
    deck_id: i64,
) -> Result<DeckProposalRules, Box<dyn std::error::Error + Send + Sync>> {
    let row = client
        .query_opt(
            "WITH RECURSIVE ancestors AS (
                 SELECT id, parent, 0 AS depth FROM anki.decks WHERE id = $1
                 UNION ALL
                 SELECT d.id, d.parent, a.depth + 1 FROM anki.decks d INNER JOIN ancestors a ON d.id = a.parent
             )
             SELECT r.deck, r.strip_colors, r.strip_span, r.strip_inline_styles, r.strip_font_tags,
                    r.prevent_center_on_back, r.trim_trailing_breaks, r.normalize_spaces,
                    r.auto_repair_cloze, r.strip_empty_elements, r.auto_tag_new_cards,
                    r.new_card_tag_pattern, r.auto_tag_updated_cards, r.updated_card_tag_pattern,
                    r.prune_old_update_tags, r.old_update_tag_pattern, r.forbidden_tags,
                    r.normalize_tag_casing, r.protected_tag_namespaces, r.enable_media_compression,
                    r.max_image_dimension, r.image_quality, r.auto_convert_heavy_images,
                    r.strip_exif_metadata, r.max_media_file_size_mb, r.require_rationale,
                    r.disallow_deletion_suggestions, r.max_cards_per_proposal,
                    r.custom_html_rules, r.custom_tag_rules
             FROM anki.deck_proposal_rules r
             JOIN ancestors a ON r.deck = a.id
             ORDER BY a.depth ASC
             LIMIT 1",
            &[&deck_id],
        )
        .await?;

    if let Some(r) = row {
        Ok(DeckProposalRules {
            deck: r.get(0),
            strip_colors: r.get(1),
            strip_span: r.get(2),
            strip_inline_styles: r.get(3),
            strip_font_tags: r.get(4),
            prevent_center_on_back: r.get(5),
            trim_trailing_breaks: r.get(6),
            normalize_spaces: r.get(7),
            auto_repair_cloze: r.get(8),
            strip_empty_elements: r.get(9),
            auto_tag_new_cards: r.get(10),
            new_card_tag_pattern: r.get(11),
            auto_tag_updated_cards: r.get(12),
            updated_card_tag_pattern: r.get(13),
            prune_old_update_tags: r.get(14),
            old_update_tag_pattern: r.get(15),
            forbidden_tags: r.get(16),
            normalize_tag_casing: r.get(17),
            protected_tag_namespaces: r.get(18),
            enable_media_compression: r.get(19),
            max_image_dimension: r.get(20),
            image_quality: r.get(21),
            auto_convert_heavy_images: r.get(22),
            strip_exif_metadata: r.get(23),
            max_media_file_size_mb: r.get(24),
            require_rationale: r.get(25),
            disallow_deletion_suggestions: r.get(26),
            max_cards_per_proposal: r.get(27),
            custom_html_rules: r.get(28),
            custom_tag_rules: r.get(29),
        })
    } else {
        let mut def = DeckProposalRules::default();
        def.deck = deck_id;
        Ok(def)
    }
}

/// Fetch deck proposal rules from the database or return the default configuration.
pub async fn get_rules(
    client: &mut SharedConn,
    deck_id: i64,
) -> Result<DeckProposalRules, Box<dyn std::error::Error + Send + Sync>> {
    get_rules_with_client(&**client, deck_id).await
}

static SPAN_OPEN_RE: std::sync::LazyLock<Regex> =
    std::sync::LazyLock::new(|| Regex::new(r"(?i)<span(?:\s+[^>]*)?>").unwrap());
static SPAN_CLOSE_RE: std::sync::LazyLock<Regex> =
    std::sync::LazyLock::new(|| Regex::new(r"(?i)</span>").unwrap());
static FONT_OPEN_RE: std::sync::LazyLock<Regex> =
    std::sync::LazyLock::new(|| Regex::new(r"(?i)<font(?:\s+[^>]*)?>").unwrap());
static FONT_CLOSE_RE: std::sync::LazyLock<Regex> =
    std::sync::LazyLock::new(|| Regex::new(r"(?i)</font>").unwrap());
static STYLE_ATTR_RE: std::sync::LazyLock<Regex> = std::sync::LazyLock::new(|| {
    Regex::new(r#"(?i)\s+style\s*=\s*(?:"[^"]*"|'[^']*'|[^\s>]*)"#).unwrap()
});
static COLOR_STYLE_RE: std::sync::LazyLock<Regex> = std::sync::LazyLock::new(|| {
    Regex::new(r#"(?i)(?:background-color|color)\s*:\s*[^;"]+;?"#).unwrap()
});
static CENTER_TAG_RE: std::sync::LazyLock<Regex> =
    std::sync::LazyLock::new(|| Regex::new(r"(?i)</?center>").unwrap());
static CENTER_STYLE_RE: std::sync::LazyLock<Regex> =
    std::sync::LazyLock::new(|| Regex::new(r#"(?i)text-align\s*:\s*center;?"#).unwrap());
static CLOZE_TYPO_RE: std::sync::LazyLock<Regex> =
    std::sync::LazyLock::new(|| Regex::new(r"\{\{c(\d+):([^\}:][^\}]*)\}\}").unwrap());
static TRAILING_BREAKS_RE: std::sync::LazyLock<Regex> = std::sync::LazyLock::new(|| {
    Regex::new(r"(?i)(?:<br\s*/?>|<div>\s*<br\s*/?>\s*</div>|\s)+$").unwrap()
});
static EMPTY_TAGS_RE: std::sync::LazyLock<Regex> = std::sync::LazyLock::new(|| {
    Regex::new(r"(?i)<(b|strong)[^>]*>\s*</(?:b|strong)>|<(i|em)[^>]*>\s*</(?:i|em)>|<p[^>]*>\s*</p>|<div[^>]*>\s*</div>|<span[^>]*>\s*</span>").unwrap()
});

/// Sanitize incoming proposed content according to deck proposal rules.
pub fn sanitize_proposal_content(raw: &str, is_back: bool, rules: &DeckProposalRules) -> String {
    sanitize_proposal_content_with_compiled(raw, is_back, rules, None)
}

fn sanitize_proposal_content_base(raw: &str, is_back: bool, rules: &DeckProposalRules) -> String {
    if raw.trim().is_empty() {
        return String::new();
    }

    let mut result = raw.to_string();

    // 1. Normalize spaces
    if rules.normalize_spaces {
        result = result
            .replace("&nbsp;", " ")
            .replace('\u{00A0}', " ")
            .replace('\u{200B}', "");
    }

    // 2. Cloze auto-repair
    if rules.auto_repair_cloze {
        result = CLOZE_TYPO_RE
            .replace_all(&result, "{{c$1::$2}}")
            .to_string();
    }

    // 3. Strip or unwrap <span>
    if rules.strip_span {
        result = SPAN_OPEN_RE.replace_all(&result, "").to_string();
        result = SPAN_CLOSE_RE.replace_all(&result, "").to_string();
    }

    // 4. Strip or unwrap <font>
    if rules.strip_font_tags {
        result = FONT_OPEN_RE.replace_all(&result, "").to_string();
        result = FONT_CLOSE_RE.replace_all(&result, "").to_string();
    }

    // 5. Strip all inline styles or strip colors
    if rules.strip_inline_styles {
        result = STYLE_ATTR_RE.replace_all(&result, "").to_string();
    } else if rules.strip_colors {
        result = STYLE_ATTR_RE
            .replace_all(&result, |caps: &regex::Captures| {
                let full = caps.get(0).map_or("", |m| m.as_str());
                let cleaned = COLOR_STYLE_RE.replace_all(full, "").to_string();
                if cleaned.contains("style=\"\"")
                    || cleaned.contains("style=''")
                    || cleaned.trim() == "style="
                {
                    String::new()
                } else {
                    cleaned
                }
            })
            .to_string();
    }

    // 6. Prevent center on back
    if is_back && rules.prevent_center_on_back {
        result = CENTER_TAG_RE.replace_all(&result, "").to_string();
        result = STYLE_ATTR_RE
            .replace_all(&result, |caps: &regex::Captures| {
                let full = caps.get(0).map_or("", |m| m.as_str());
                let cleaned = CENTER_STYLE_RE.replace_all(full, "").to_string();
                if cleaned.contains("style=\"\"")
                    || cleaned.contains("style=''")
                    || cleaned.trim() == "style="
                {
                    String::new()
                } else {
                    cleaned
                }
            })
            .to_string();
    }

    // 7. Strip empty elements
    if rules.strip_empty_elements {
        for _ in 0..3 {
            let next = EMPTY_TAGS_RE.replace_all(&result, "").to_string();
            if next == result {
                break;
            }
            result = next;
        }
    }

    // 8. Trim trailing linebreaks & empty divs
    if rules.trim_trailing_breaks {
        result = TRAILING_BREAKS_RE.replace(&result, "").to_string();
    }

    result
}

/// Parse and execute line-by-line custom HTML rules (.config syntax).
#[derive(Clone, Debug)]
pub enum CompiledHtmlDirective {
    StripTags { open: Regex, close: Regex },
    RemoveElements(Regex),
    StripStyles(Regex),
    StripAttributes(Regex),
    StripEmpty(Regex),
    Replace { from: String, to: String },
    RegexReplace { re: Regex, to: String },
}

#[derive(Clone, Debug, Default)]
pub struct CompiledCustomHtmlRules {
    directives: Vec<CompiledHtmlDirective>,
}

impl CompiledCustomHtmlRules {
    pub fn compile(custom_rules: &str) -> Self {
        const MAX_DIRECTIVES: usize = 64;
        const REGEX_SIZE_LIMIT: usize = 256 * 1024;
        let build_regex = |pat: &str| {
            RegexBuilder::new(pat)
                .size_limit(REGEX_SIZE_LIMIT)
                .dfa_size_limit(REGEX_SIZE_LIMIT)
                .build()
        };

        let mut directives = Vec::new();
        for line in custom_rules.lines().take(MAX_DIRECTIVES) {
            let trimmed = line.trim();
            if trimmed.is_empty() || trimmed.starts_with('#') {
                continue;
            }
            if directives.len() >= MAX_DIRECTIVES {
                break;
            }

            if let Some((directive, args)) = trimmed.split_once(':') {
                let dir = directive.trim().to_lowercase();
                let args = args.trim();

                match dir.as_str() {
                    "strip-tags" | "unwrap-tags" => {
                        for tag in args.split(',').take(MAX_DIRECTIVES) {
                            let t = tag.trim().to_lowercase();
                            if !t.is_empty() && directives.len() < MAX_DIRECTIVES {
                                let open_pattern =
                                    format!(r"(?i)<{}(?:\s+[^>]*)?>", regex::escape(&t));
                                let close_pattern = format!(r"(?i)</{}>", regex::escape(&t));
                                if let (Ok(o), Ok(c)) =
                                    (build_regex(&open_pattern), build_regex(&close_pattern))
                                {
                                    directives.push(CompiledHtmlDirective::StripTags {
                                        open: o,
                                        close: c,
                                    });
                                }
                            }
                        }
                    }
                    "remove-elements" => {
                        for tag in args.split(',').take(MAX_DIRECTIVES) {
                            let t = tag.trim().to_lowercase();
                            if !t.is_empty() && directives.len() < MAX_DIRECTIVES {
                                let pattern = format!(
                                    r"(?i)<{}(?:\s+[^>]*)?>.*?</{}>",
                                    regex::escape(&t),
                                    regex::escape(&t)
                                );
                                if let Ok(re) = build_regex(&pattern) {
                                    directives.push(CompiledHtmlDirective::RemoveElements(re));
                                }
                            }
                        }
                    }
                    "strip-styles" => {
                        for prop in args.split(',').take(MAX_DIRECTIVES) {
                            let p = prop.trim().to_lowercase();
                            if !p.is_empty() && directives.len() < MAX_DIRECTIVES {
                                let pattern =
                                    format!(r#"(?i){}\s*:\s*[^;"]+;?"#, regex::escape(&p));
                                if let Ok(re) = build_regex(&pattern) {
                                    directives.push(CompiledHtmlDirective::StripStyles(re));
                                }
                            }
                        }
                    }
                    "strip-attributes" => {
                        for attr in args.split(',').take(MAX_DIRECTIVES) {
                            let a = attr.trim().to_lowercase();
                            if !a.is_empty() && directives.len() < MAX_DIRECTIVES {
                                let pattern = format!(
                                    r#"(?i)\s+{}\s*=\s*(?:"[^"]*"|'[^']*'|[^\s>]*)"#,
                                    regex::escape(&a)
                                );
                                if let Ok(re) = build_regex(&pattern) {
                                    directives.push(CompiledHtmlDirective::StripAttributes(re));
                                }
                            }
                        }
                    }
                    "strip-empty" => {
                        for tag in args.split(',').take(MAX_DIRECTIVES) {
                            let t = tag.trim().to_lowercase();
                            if !t.is_empty() && directives.len() < MAX_DIRECTIVES {
                                let pattern = format!(
                                    r"(?i)<{}(?:\s+[^>]*)?>\s*</{}>",
                                    regex::escape(&t),
                                    regex::escape(&t)
                                );
                                if let Ok(re) = build_regex(&pattern) {
                                    directives.push(CompiledHtmlDirective::StripEmpty(re));
                                }
                            }
                        }
                    }
                    "replace" => {
                        if let Some((from_part, to_part)) = args.split_once("=>") {
                            let from_str = from_part.trim().trim_matches('"').trim_matches('\'');
                            let to_str = to_part.trim().trim_matches('"').trim_matches('\'');
                            if !from_str.is_empty() && directives.len() < MAX_DIRECTIVES {
                                directives.push(CompiledHtmlDirective::Replace {
                                    from: from_str.to_string(),
                                    to: to_str.to_string(),
                                });
                            }
                        }
                    }
                    "regex-replace" => {
                        if let Some((from_part, to_part)) = args.split_once("=>") {
                            let pat_raw = from_part.trim();
                            let to_str = to_part.trim().trim_matches('"').trim_matches('\'');
                            let pattern = pat_raw
                                .strip_prefix('/')
                                .and_then(|s| s.strip_suffix('/'))
                                .unwrap_or(pat_raw);
                            if directives.len() < MAX_DIRECTIVES {
                                if let Ok(re) = build_regex(pattern) {
                                    directives.push(CompiledHtmlDirective::RegexReplace {
                                        re,
                                        to: to_str.to_string(),
                                    });
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        }
        Self { directives }
    }

    pub fn apply(&self, mut html: String) -> String {
        const MAX_HTML_OUTPUT_BYTES: usize = 512 * 1024;
        for directive in &self.directives {
            match directive {
                CompiledHtmlDirective::StripTags { open, close } => {
                    html = open.replace_all(&html, "").to_string();
                    html = close.replace_all(&html, "").to_string();
                }
                CompiledHtmlDirective::RemoveElements(re) => {
                    html = re.replace_all(&html, "").to_string();
                }
                CompiledHtmlDirective::StripStyles(re) => {
                    html = STYLE_ATTR_RE
                        .replace_all(&html, |caps: &regex::Captures| {
                            let full = caps.get(0).map_or("", |m| m.as_str());
                            let cleaned = re.replace_all(full, "").to_string();
                            if cleaned.contains("style=\"\"")
                                || cleaned.contains("style=''")
                                || cleaned.trim() == "style="
                            {
                                String::new()
                            } else {
                                cleaned
                            }
                        })
                        .to_string();
                }
                CompiledHtmlDirective::StripAttributes(re) => {
                    html = re.replace_all(&html, "").to_string();
                }
                CompiledHtmlDirective::StripEmpty(re) => {
                    for _ in 0..3 {
                        let next = re.replace_all(&html, "").to_string();
                        if next == html {
                            break;
                        }
                        html = next;
                    }
                }
                CompiledHtmlDirective::Replace { from, to } => {
                    let next = html.replace(from, to);
                    if next.len() <= MAX_HTML_OUTPUT_BYTES {
                        html = next;
                    }
                }
                CompiledHtmlDirective::RegexReplace { re, to } => {
                    let next = re.replace_all(&html, to.as_str()).to_string();
                    if next.len() <= MAX_HTML_OUTPUT_BYTES {
                        html = next;
                    }
                }
            }
        }
        html
    }
}

impl DeckProposalRules {
    pub fn compile_custom_html(&self) -> Option<CompiledCustomHtmlRules> {
        if self.custom_html_rules.trim().is_empty() {
            None
        } else {
            Some(CompiledCustomHtmlRules::compile(&self.custom_html_rules))
        }
    }
}

pub fn sanitize_proposal_content_with_compiled(
    raw: &str,
    is_back: bool,
    rules: &DeckProposalRules,
    compiled: Option<&CompiledCustomHtmlRules>,
) -> String {
    let mut result = sanitize_proposal_content_base(raw, is_back, rules);
    if let Some(c) = compiled {
        result = c.apply(result);
    } else if !rules.custom_html_rules.trim().is_empty() {
        result = apply_custom_html_rules(result, &rules.custom_html_rules);
    }
    result
}

/// Parse and execute line-by-line custom HTML rules (.config syntax).
pub fn apply_custom_html_rules(html: String, custom_rules: &str) -> String {
    let compiled = CompiledCustomHtmlRules::compile(custom_rules);
    compiled.apply(html)
}

/// Parse and execute line-by-line custom Tag rules (.config syntax).
pub fn apply_custom_tag_rules(mut tags: Vec<String>, custom_rules: &str) -> Vec<String> {
    for line in custom_rules.lines() {
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }

        if let Some((directive, args)) = trimmed.split_once(':') {
            let dir = directive.trim().to_lowercase();
            let args = args.trim();

            match dir.as_str() {
                "block" | "remove" => {
                    for pat in args.split(',') {
                        let p = pat.trim().to_lowercase();
                        if !p.is_empty() {
                            tags.retain(|t| {
                                let t_low = t.to_lowercase();
                                if let Some(prefix) = p.strip_suffix('*') {
                                    !t_low.starts_with(prefix)
                                } else {
                                    t_low != p
                                }
                            });
                        }
                    }
                }
                "allow-only" => {
                    let patterns: Vec<String> = args
                        .split(',')
                        .map(|s| s.trim().to_lowercase())
                        .filter(|s| !s.is_empty())
                        .collect();
                    if !patterns.is_empty() {
                        tags.retain(|t| {
                            let t_low = t.to_lowercase();
                            patterns.iter().any(|p| {
                                if let Some(prefix) = p.strip_suffix('*') {
                                    t_low.starts_with(prefix)
                                } else {
                                    &t_low == p
                                }
                            })
                        });
                    }
                }
                "replace" => {
                    if let Some((from_part, to_part)) = args.split_once("=>") {
                        let from_str = from_part
                            .trim()
                            .trim_matches('"')
                            .trim_matches('\'')
                            .to_string();
                        let to_str = to_part
                            .trim()
                            .trim_matches('"')
                            .trim_matches('\'')
                            .to_string();
                        for t in &mut tags {
                            if *t == from_str {
                                *t = to_str.clone();
                            }
                        }
                    }
                }
                "prefix-all" => {
                    let prefix = args.trim().trim_matches('"').trim_matches('\'');
                    if !prefix.is_empty() {
                        for t in &mut tags {
                            if !t.starts_with(prefix) {
                                *t = format!("{prefix}{t}");
                            }
                        }
                    }
                }
                "case" => {
                    let c = args.trim().to_lowercase();
                    if c == "lower" || c == "lowercase" {
                        for t in &mut tags {
                            *t = t.to_lowercase();
                        }
                    } else if c == "upper" || c == "uppercase" {
                        for t in &mut tags {
                            *t = t.to_uppercase();
                        }
                    }
                }
                _ => {}
            }
        }
    }
    tags
}

/// Filter out tags that match the deck's forbidden tags blacklist.
pub fn is_tag_forbidden(tag: &str, forbidden_tags: &str) -> bool {
    let clean_tag = tag.trim().to_lowercase();
    if clean_tag.is_empty() {
        return false;
    }

    for item in forbidden_tags.split(',') {
        let pattern = item.trim().to_lowercase();
        if pattern.is_empty() {
            continue;
        }
        if let Some(prefix) = pattern.strip_suffix('*') {
            if clean_tag.starts_with(prefix) {
                return true;
            }
        } else if clean_tag == pattern {
            return true;
        }
    }
    false
}

/// Check if a tag belongs to any protected namespace (e.g. OFFICIAL::*, EDN::*).
pub fn is_tag_protected(tag: &str, protected_namespaces: &str) -> bool {
    let clean_tag = tag.trim().to_lowercase();
    if clean_tag.is_empty() || protected_namespaces.trim().is_empty() {
        return false;
    }

    for item in protected_namespaces.split(',') {
        let pattern = item.trim().to_lowercase();
        if pattern.is_empty() {
            continue;
        }
        if let Some(prefix) = pattern.strip_suffix('*') {
            if clean_tag.starts_with(prefix) {
                return true;
            }
        } else if clean_tag == pattern {
            return true;
        }
    }
    false
}

/// Resolve date placeholder tokens ({YYYY}, {YY}, {MM}, {DD}, {YYYY-MM}, {MM/YY}, {MM-YY}) in tag patterns.
pub fn resolve_date_pattern(pattern: &str) -> String {
    let now = chrono::Utc::now();
    let yyyy = now.format("%Y").to_string();
    let yy = now.format("%y").to_string();
    let mm = now.format("%m").to_string();
    let dd = now.format("%d").to_string();
    let yyyy_mm = format!("{yyyy}-{mm}");
    let mm_slash_yy = format!("{mm}/{yy}");
    let mm_dash_yy = format!("{mm}-{yy}");

    pattern
        .replace("{YYYY-MM}", &yyyy_mm)
        .replace("{yyyy-mm}", &yyyy_mm)
        .replace("{MM/YY}", &mm_slash_yy)
        .replace("{mm/yy}", &mm_slash_yy)
        .replace("{MM-YY}", &mm_dash_yy)
        .replace("{mm-yy}", &mm_dash_yy)
        .replace("{YYYY}", &yyyy)
        .replace("{yyyy}", &yyyy)
        .replace("{YY}", &yy)
        .replace("{yy}", &yy)
        .replace("{MM}", &mm)
        .replace("{mm}", &mm)
        .replace("{DD}", &dd)
        .replace("{dd}", &dd)
}

/// Apply automated tagging and old-tag pruning when a card proposal is approved.
pub async fn apply_tagging_rules_on_approval(
    tx: &tokio_postgres::Transaction<'_>,
    note_id: i64,
    is_new: bool,
    rules: &DeckProposalRules,
    commit_id: Option<i32>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if is_new && rules.auto_tag_new_cards {
        let tag_name = resolve_date_pattern(&rules.new_card_tag_pattern);
        let tag_trimmed = tag_name.trim();
        if !tag_trimmed.is_empty() {
            tx.execute(
                "INSERT INTO anki.tags (note, content, commit, reviewed, action) \
                 SELECT $1, $2, $3, true, true \
                 WHERE NOT EXISTS ( \
                     SELECT 1 FROM anki.tags WHERE note = $1 AND content = $2 AND reviewed = true \
                 )",
                &[&note_id, &tag_trimmed, &commit_id],
            )
            .await?;
        }
    } else if !is_new && rules.auto_tag_updated_cards {
        let tag_name = resolve_date_pattern(&rules.updated_card_tag_pattern);
        let tag_trimmed = tag_name.trim();
        if !tag_trimmed.is_empty() {
            if rules.prune_old_update_tags {
                let pattern = rules.old_update_tag_pattern.trim();
                let meaningful_base = pattern.trim_matches('*').trim();
                if !meaningful_base.is_empty() {
                    let escaped = pattern
                        .replace('\\', "\\\\")
                        .replace('%', "\\%")
                        .replace('_', "\\_");
                    let like_pattern = if let Some(prefix) = escaped.strip_suffix('*') {
                        format!("{}%", prefix.replace('*', "%"))
                    } else if escaped.contains('*') {
                        escaped.replace('*', "%")
                    } else {
                        format!("{escaped}%")
                    };

                    tx.execute(
                        "DELETE FROM anki.tags \
                         WHERE note = $1 AND content LIKE $2 ESCAPE '\\' AND content <> $3",
                        &[&note_id, &like_pattern, &tag_trimmed],
                    )
                    .await?;
                }
            }

            tx.execute(
                "INSERT INTO anki.tags (note, content, commit, reviewed, action) \
                 SELECT $1, $2, $3, true, true \
                 WHERE NOT EXISTS ( \
                     SELECT 1 FROM anki.tags WHERE note = $1 AND content = $2 AND reviewed = true \
                 )",
                &[&note_id, &tag_trimmed, &commit_id],
            )
            .await?;
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_natively_off_default() {
        let rules = DeckProposalRules::default();
        assert!(!rules.strip_colors);
        assert!(!rules.strip_span);
        assert!(!rules.strip_inline_styles);
        assert!(!rules.strip_font_tags);
        assert!(!rules.prevent_center_on_back);
        assert!(!rules.trim_trailing_breaks);
        assert!(!rules.normalize_spaces);
        assert!(!rules.auto_repair_cloze);
        assert!(!rules.strip_empty_elements);
        assert!(!rules.auto_tag_new_cards);
        assert!(!rules.auto_tag_updated_cards);
        assert!(!rules.prune_old_update_tags);
        assert!(!rules.enable_media_compression);
        assert!(!rules.auto_convert_heavy_images);
        assert!(!rules.strip_exif_metadata);
        assert!(!rules.require_rationale);
        assert!(!rules.disallow_deletion_suggestions);
        assert_eq!(rules.max_cards_per_proposal, 0);
        assert_eq!(rules.max_media_file_size_mb, 0);
        assert_eq!(rules.custom_html_rules, "");
        assert_eq!(rules.custom_tag_rules, "");
    }

    #[test]
    fn test_unwrap_span_preserves_content() {
        let mut rules = DeckProposalRules::default();
        rules.strip_span = true;
        let input =
            r#"Plain text <span style="font-size: 14px; color: red;">important word</span> after."#;
        let output = sanitize_proposal_content(input, false, &rules);
        assert_eq!(output, "Plain text important word after.");
    }

    #[test]
    fn test_strip_font_tags() {
        let mut rules = DeckProposalRules::default();
        rules.strip_font_tags = true;
        let input = r##"<font color="#ff0000" size="4">Red text</font>"##;
        let output = sanitize_proposal_content(input, false, &rules);
        assert_eq!(output, "Red text");
    }

    #[test]
    fn test_prevent_center_on_back() {
        let mut rules = DeckProposalRules::default();
        rules.prevent_center_on_back = true;
        let input =
            r#"<center>Centered text</center><div style="text-align: center;">Aligned text</div>"#;
        let output = sanitize_proposal_content(input, true, &rules);
        assert!(!output.contains("<center>"));
        assert!(!output.contains("text-align: center"));
        assert!(output.contains("Centered text"));
        assert!(output.contains("Aligned text"));
    }

    #[test]
    fn test_cloze_auto_repair() {
        let mut rules = DeckProposalRules::default();
        rules.auto_repair_cloze = true;
        let input = "The {{c1:first}} cloze and {{c2:second}} cloze.";
        let output = sanitize_proposal_content(input, false, &rules);
        assert_eq!(output, "The {{c1::first}} cloze and {{c2::second}} cloze.");

        // Ensure already valid clozes are not mutated into three colons
        let valid_input = "The {{c1::first}} valid cloze.";
        let valid_output = sanitize_proposal_content(valid_input, false, &rules);
        assert_eq!(valid_output, "The {{c1::first}} valid cloze.");
    }

    #[test]
    fn test_trim_trailing_breaks() {
        let mut rules = DeckProposalRules::default();
        rules.trim_trailing_breaks = true;
        let input = "Valid content<br><br><div><br></div>  ";
        let output = sanitize_proposal_content(input, false, &rules);
        assert_eq!(output, "Valid content");
    }

    #[test]
    fn test_custom_html_rules() {
        let mut rules = DeckProposalRules::default();
        rules.custom_html_rules = r#"
            strip-tags: span, em
            remove-elements: script
            strip-styles: color
            replace: "banana" => "apple"
        "#
        .to_string();

        let input = r#"<span>Good <script>evil()</script><em>taste</em> with banana and <div style="color: blue; font-size: 12px;">sweet</div></span>"#;
        let output = sanitize_proposal_content(input, false, &rules);

        assert!(!output.contains("<span>"));
        assert!(!output.contains("<em>"));
        assert!(!output.contains("<script>"));
        assert!(!output.contains("color: blue"));
        assert!(output.contains("font-size: 12px"));
        assert!(output.contains("apple"));
    }

    #[test]
    fn test_custom_tag_rules() {
        let custom_rules = r#"
            block: test*, bad
            replace: "old" => "new"
            prefix-all: "MED::"
            case: lower
        "#;
        let tags = vec![
            "test1".to_string(),
            "bad".to_string(),
            "old".to_string(),
            "Cardio".to_string(),
        ];
        let res = apply_custom_tag_rules(tags, custom_rules);

        assert!(!res.contains(&"test1".to_string()));
        assert!(!res.contains(&"bad".to_string()));
        assert!(res.contains(&"med::new".to_string()));
        assert!(res.contains(&"med::cardio".to_string()));
    }

    #[test]
    fn test_is_tag_forbidden() {
        let blacklist = "marked, leech, flag*, todo*";
        assert!(is_tag_forbidden("marked", blacklist));
        assert!(is_tag_forbidden("LEECH", blacklist));
        assert!(is_tag_forbidden("flag1", blacklist));
        assert!(is_tag_forbidden("flag-blue", blacklist));
        assert!(is_tag_forbidden("todo-later", blacklist));
        assert!(!is_tag_forbidden("cardiology", blacklist));
        assert!(!is_tag_forbidden("EDN::Cardio", blacklist));
    }

    #[test]
    fn test_is_tag_protected() {
        assert!(is_tag_protected("OFFICIAL::Card", "OFFICIAL::*, EDN::*"));
        assert!(is_tag_protected("edn::item1", "OFFICIAL::*, EDN::*"));
        assert!(is_tag_protected("core", "core, extra"));
        assert!(!is_tag_protected("custom::tag", "OFFICIAL::*, EDN::*"));
        assert!(!is_tag_protected("any", ""));
    }

    #[test]
    fn test_resolve_date_pattern() {
        let pattern = "MAJ::{YYYY}_{MM}";
        let resolved = resolve_date_pattern(pattern);
        assert!(!resolved.contains("{YYYY}"));
        assert!(!resolved.contains("{MM}"));
        assert!(resolved.starts_with("MAJ::"));
    }
}
