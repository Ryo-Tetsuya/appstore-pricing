use anyhow::{anyhow, Context, Result};
use eframe::egui;
use egui::{Color32, RichText};
use egui_extras::{Column, TableBuilder};
use futures::stream::{self, StreamExt};
use html_escape::decode_html_entities;
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::OnceLock;
use std::time::Duration;
use tokio::runtime::{Builder as RuntimeBuilder, Runtime};

const BASE_PRICE_CONCURRENCY: usize = 32;
const IAP_PRICE_CONCURRENCY: usize = 48;
const IAP_PREFETCH_CONCURRENCY: usize = 16;
const IAP_PROGRESS_BATCH_SIZE: usize = 8;
const HTTP_POOL_MAX_IDLE_PER_HOST: usize = IAP_PRICE_CONCURRENCY;
const TOKIO_WORKER_THREADS: usize = 4;
const HTTP_TIMEOUT_SECS: u64 = 15;
const HTTP_CONNECT_TIMEOUT_SECS: u64 = 6;
const APPLE_COUNTRY_REGION_URL: &str = "https://www.apple.com/choose-country-region/";
const CONTROL_HEIGHT: f32 = 32.0;
const APP_CONTENT_WIDTH: f32 = 1180.0;
const APP_WINDOW_WIDTH: f32 = 1240.0;
const APP_ICON_SIZE: u32 = 128;
const FRAME_RADIUS: u8 = 2;
const CONTROL_RADIUS: u8 = 2;
const ROW_RADIUS: f32 = 2.0;
const TABLE_HEADER_RADIUS: f32 = 0.0;

#[derive(Copy, Clone)]
struct Region {
    code: &'static str,
    name: &'static str,
}

#[rustfmt::skip]
const REGIONS: &[Region] = &[
    // Africa, Middle East, and India
    Region { code: "DZ", name: "Algeria" },
    Region { code: "AO", name: "Angola" },
    Region { code: "BJ", name: "Benin" },
    Region { code: "BW", name: "Botswana" },
    Region { code: "BF", name: "Burkina Faso" },
    Region { code: "BH", name: "Bahrain" },
    Region { code: "CM", name: "Cameroon" },
    Region { code: "CF", name: "Central African Republic" },
    Region { code: "CI", name: "Côte d’Ivoire" },
    Region { code: "CD", name: "Democratic Republic of the Congo" },
    Region { code: "EG", name: "Egypt" },
    Region { code: "GQ", name: "Equatorial Guinea" },
    Region { code: "GH", name: "Ghana" },
    Region { code: "GN", name: "Guinea" },
    Region { code: "GW", name: "Guinea-Bissau" },
    Region { code: "IN", name: "India" },
    Region { code: "IL", name: "Israel" },
    Region { code: "JO", name: "Jordan" },
    Region { code: "KE", name: "Kenya" },
    Region { code: "KW", name: "Kuwait" },
    Region { code: "LR", name: "Liberia" },
    Region { code: "LY", name: "Libya" },
    Region { code: "MG", name: "Madagascar" },
    Region { code: "MW", name: "Malawi" },
    Region { code: "ML", name: "Mali" },
    Region { code: "MR", name: "Mauritania" },
    Region { code: "MU", name: "Mauritius" },
    Region { code: "MA", name: "Morocco" },
    Region { code: "MZ", name: "Mozambique" },
    Region { code: "NA", name: "Namibia" },
    Region { code: "NE", name: "Niger" },
    Region { code: "NG", name: "Nigeria" },
    Region { code: "OM", name: "Oman" },
    Region { code: "PK", name: "Pakistan" },
    Region { code: "QA", name: "Qatar" },
    Region { code: "RW", name: "Rwanda" },
    Region { code: "SA", name: "Saudi Arabia" },
    Region { code: "SN", name: "Senegal" },
    Region { code: "SC", name: "Seychelles" },
    Region { code: "SL", name: "Sierra Leone" },
    Region { code: "ZA", name: "South Africa" },
    Region { code: "TZ", name: "Tanzania" },
    Region { code: "TN", name: "Tunisia" },
    Region { code: "UG", name: "Uganda" },
    Region { code: "AE", name: "United Arab Emirates" },
    Region { code: "ZM", name: "Zambia" },
    Region { code: "ZW", name: "Zimbabwe" },
    // Asia Pacific
    Region { code: "AU", name: "Australia" },
    Region { code: "BD", name: "Bangladesh" },
    Region { code: "BT", name: "Bhutan" },
    Region { code: "BN", name: "Brunei Darussalam" },
    Region { code: "KH", name: "Cambodia" },
    Region { code: "CN", name: "China" },
    Region { code: "FJ", name: "Fiji" },
    Region { code: "HK", name: "Hong Kong" },
    Region { code: "ID", name: "Indonesia" },
    Region { code: "JP", name: "Japan" },
    Region { code: "KZ", name: "Kazakhstan" },
    Region { code: "KG", name: "Kyrgyzstan" },
    Region { code: "MO", name: "Macau" },
    Region { code: "MY", name: "Malaysia" },
    Region { code: "MV", name: "Maldives" },
    Region { code: "MN", name: "Mongolia" },
    Region { code: "MM", name: "Myanmar" },
    Region { code: "NP", name: "Nepal" },
    Region { code: "NZ", name: "New Zealand" },
    Region { code: "PH", name: "Philippines" },
    Region { code: "SG", name: "Singapore" },
    Region { code: "KR", name: "South Korea" },
    Region { code: "LK", name: "Sri Lanka" },
    Region { code: "TW", name: "Taiwan" },
    Region { code: "TJ", name: "Tajikistan" },
    Region { code: "TH", name: "Thailand" },
    Region { code: "TM", name: "Turkmenistan" },
    Region { code: "UZ", name: "Uzbekistan" },
    // Europe
    Region { code: "VN", name: "Vietnam" },
    Region { code: "AL", name: "Albania" },
    Region { code: "AM", name: "Armenia" },
    Region { code: "AT", name: "Austria" },
    Region { code: "AZ", name: "Azerbaijan" },
    Region { code: "BY", name: "Belarus" },
    Region { code: "BE", name: "Belgium" },
    Region { code: "BA", name: "Bosnia and Herzegovina" },
    Region { code: "BG", name: "Bulgaria" },
    Region { code: "HR", name: "Croatia" },
    Region { code: "CY", name: "Cyprus" },
    Region { code: "CZ", name: "Czech Republic" },
    Region { code: "DK", name: "Denmark" },
    Region { code: "EE", name: "Estonia" },
    Region { code: "FI", name: "Finland" },
    Region { code: "FR", name: "France" },
    Region { code: "GE", name: "Georgia" },
    Region { code: "DE", name: "Germany" },
    Region { code: "GR", name: "Greece" },
    Region { code: "HU", name: "Hungary" },
    Region { code: "IS", name: "Iceland" },
    Region { code: "IE", name: "Ireland" },
    Region { code: "IT", name: "Italy" },
    Region { code: "XK", name: "Kosovo" },
    Region { code: "LV", name: "Latvia" },
    Region { code: "LI", name: "Liechtenstein" },
    Region { code: "LT", name: "Lithuania" },
    Region { code: "LU", name: "Luxembourg" },
    Region { code: "MT", name: "Malta" },
    Region { code: "MD", name: "Moldova" },
    Region { code: "ME", name: "Montenegro" },
    Region { code: "NL", name: "Netherlands" },
    Region { code: "MK", name: "North Macedonia" },
    Region { code: "NO", name: "Norway" },
    Region { code: "PL", name: "Poland" },
    Region { code: "PT", name: "Portugal" },
    Region { code: "RO", name: "Romania" },
    Region { code: "RU", name: "Russia" },
    Region { code: "SK", name: "Slovakia" },
    Region { code: "SI", name: "Slovenia" },
    Region { code: "ES", name: "Spain" },
    Region { code: "SE", name: "Sweden" },
    Region { code: "CH", name: "Switzerland" },
    Region { code: "TR", name: "Turkey" },
    Region { code: "UA", name: "Ukraine" },
    Region { code: "GB", name: "United Kingdom" },
    Region { code: "AI", name: "Anguilla" },
    Region { code: "AG", name: "Antigua and Barbuda" },
    Region { code: "AR", name: "Argentina" },
    Region { code: "BS", name: "Bahamas" },
    Region { code: "BB", name: "Barbados" },
    Region { code: "BZ", name: "Belize" },
    Region { code: "BM", name: "Bermuda" },
    Region { code: "BO", name: "Bolivia" },
    Region { code: "BR", name: "Brazil" },
    Region { code: "VG", name: "British Virgin Islands" },
    // Latin America and the Caribbean
    Region { code: "KY", name: "Cayman Islands" },
    Region { code: "CL", name: "Chile" },
    Region { code: "CO", name: "Colombia" },
    Region { code: "CR", name: "Costa Rica" },
    Region { code: "DM", name: "Dominica" },
    Region { code: "DO", name: "Dominican Republic" },
    Region { code: "EC", name: "Ecuador" },
    Region { code: "SV", name: "El Salvador" },
    Region { code: "GD", name: "Grenada" },
    Region { code: "GT", name: "Guatemala" },
    Region { code: "GY", name: "Guyana" },
    Region { code: "HN", name: "Honduras" },
    Region { code: "JM", name: "Jamaica" },
    Region { code: "MX", name: "Mexico" },
    Region { code: "MS", name: "Montserrat" },
    Region { code: "NI", name: "Nicaragua" },
    Region { code: "PA", name: "Panama" },
    Region { code: "PY", name: "Paraguay" },
    Region { code: "PE", name: "Peru" },
    Region { code: "KN", name: "St. Kitts & Nevis" },
    Region { code: "LC", name: "St. Lucia" },
    Region { code: "VC", name: "St. Vincent & The Grenadines" },
    Region { code: "SR", name: "Suriname" },
    Region { code: "TT", name: "Trinidad & Tobago" },
    Region { code: "TC", name: "Turks & Caicos" },
    Region { code: "UY", name: "Uruguay" },
    Region { code: "VE", name: "Venezuela" },
    Region { code: "CA", name: "Canada" },
    Region { code: "US", name: "United States" },
    Region { code: "PR", name: "Puerto Rico" },
    Region { code: "FM", name: "Micronesia" },
    Region { code: "NR", name: "Nauru" },
    Region { code: "PG", name: "Papua New Guinea" },
    Region { code: "SB", name: "Solomon Islands" },
    Region { code: "TO", name: "Tonga" },
    Region { code: "VU", name: "Vanuatu" },
];

fn app_id_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"(?:^|/)id(\d+)").expect("valid app id regex"))
}

fn app_store_region_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r"apps\.apple\.com/([a-z]{2})/").expect("valid App Store region regex")
    })
}

fn country_link_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r#"(?s)<a\b([^>]*)>(.*?)</a>"#).expect("valid link regex"))
}

fn country_span_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r#"(?s)<span\b([^>]*)>(.*?)</span>"#).expect("valid span regex"))
}

fn country_meta_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r#"<meta\b([^>]*)>"#).expect("valid meta regex"))
}

fn extract_app_id(input: &str) -> Option<String> {
    let trimmed = input.trim();
    if !trimmed.is_empty() && trimmed.chars().all(|c| c.is_ascii_digit()) {
        return Some(trimmed.to_string());
    }

    app_id_regex()
        .captures(trimmed)
        .and_then(|caps| caps.get(1))
        .map(|id| id.as_str().to_string())
}

fn region_from_app_store_url(input: &str) -> Option<Region> {
    let code = app_store_region_regex()
        .captures(input.trim())
        .and_then(|caps| caps.get(1))?
        .as_str()
        .to_ascii_uppercase();

    REGIONS.iter().copied().find(|r| r.code == code)
}

#[derive(Debug, Clone)]
struct AppleCountryEntry {
    label: String,
    site_code: String,
    inferred_code: Option<String>,
}

#[derive(Debug, Clone)]
struct RegionAuditCandidate {
    code: String,
    label: String,
}

#[derive(Debug, Clone)]
struct RegionAuditReport {
    missing_candidates: Vec<RegionAuditCandidate>,
    route_aliases_by_region: HashMap<String, Vec<String>>,
}

async fn audit_apple_country_regions(client: &reqwest::Client) -> Result<RegionAuditReport> {
    let html = fetch_text(client, APPLE_COUNTRY_REGION_URL).await?;
    Ok(build_region_audit_report(&html))
}

fn build_region_audit_report(html: &str) -> RegionAuditReport {
    let entries = parse_apple_country_entries(html);
    let configured_codes: HashSet<&str> = REGIONS.iter().map(|region| region.code).collect();
    let mut missing_candidates_by_code: HashMap<String, String> = HashMap::new();
    let mut alias_sets: HashMap<String, HashSet<String>> = HashMap::new();

    for entry in entries {
        let Some(code) = entry.inferred_code else {
            continue;
        };

        if !configured_codes.contains(code.as_str()) {
            missing_candidates_by_code
                .entry(code.clone())
                .or_insert(entry.label.clone());
        }

        let site_code = entry.site_code.to_ascii_uppercase();
        if site_code != code {
            alias_sets.entry(code).or_default().insert(site_code);
        }
    }

    let mut missing_candidates: Vec<_> = missing_candidates_by_code
        .into_iter()
        .map(|(code, label)| RegionAuditCandidate { code, label })
        .collect();
    missing_candidates.sort_by(|a, b| a.code.cmp(&b.code));

    let mut route_aliases_by_region: HashMap<String, Vec<String>> = alias_sets
        .into_iter()
        .map(|(code, aliases)| {
            let mut aliases: Vec<_> = aliases.into_iter().collect();
            aliases.sort();
            (code, aliases)
        })
        .collect();
    route_aliases_by_region.retain(|code, _| configured_codes.contains(code.as_str()));

    RegionAuditReport {
        missing_candidates,
        route_aliases_by_region,
    }
}

fn parse_apple_country_entries(html: &str) -> Vec<AppleCountryEntry> {
    country_link_regex()
        .captures_iter(html)
        .filter_map(|caps| {
            let attrs = caps.get(1)?.as_str();
            let body = caps.get(2)?.as_str();
            if !body.contains("countrylist-caption") {
                return None;
            }

            let href = html_attr_value(attrs, "href")?;
            let (label, span_attrs) = country_label_and_attrs(body)?;
            let language = country_language(body).or_else(|| html_attr_value(&span_attrs, "lang"));
            let site_code = apple_site_code_from_href(&href);
            let inferred_code = infer_apple_country_code(&label, language.as_deref(), &site_code);

            Some(AppleCountryEntry {
                label,
                site_code,
                inferred_code,
            })
        })
        .collect()
}

fn country_label_and_attrs(body: &str) -> Option<(String, String)> {
    country_span_regex().captures_iter(body).find_map(|caps| {
        let attrs = caps.get(1)?.as_str();
        if !attrs.contains("countrylist-caption") {
            return None;
        }

        let label = decode_html_entities(caps.get(2)?.as_str())
            .trim()
            .to_string();
        if label.is_empty() {
            None
        } else {
            Some((label, attrs.to_string()))
        }
    })
}

fn country_language(body: &str) -> Option<String> {
    country_meta_regex().captures_iter(body).find_map(|caps| {
        let attrs = caps.get(1)?.as_str();
        if attrs.contains("schema:inLanguage") {
            html_attr_value(attrs, "content")
        } else {
            None
        }
    })
}

fn html_attr_value(attrs: &str, name: &str) -> Option<String> {
    let needle = format!(r#"{name}=""#);
    let start = attrs.find(&needle)? + needle.len();
    let end = attrs[start..].find('"')? + start;
    Some(attrs[start..end].to_string())
}

fn apple_site_code_from_href(href: &str) -> String {
    let href = href.trim().to_ascii_lowercase();
    if href.contains("apple.com.cn") {
        return "cn".to_string();
    }

    let path = href
        .strip_prefix("https://www.apple.com")
        .or_else(|| href.strip_prefix("http://www.apple.com"))
        .unwrap_or(&href);
    path.split(['?', '#'])
        .next()
        .unwrap_or(path)
        .split('/')
        .find(|part| !part.is_empty())
        .unwrap_or("us")
        .to_string()
}

fn infer_apple_country_code(
    label: &str,
    language: Option<&str>,
    site_code: &str,
) -> Option<String> {
    if let Some(code) = country_code_from_language(language) {
        return Some(code);
    }

    if let Some(code) = country_code_from_native_label(label) {
        return Some(code.to_string());
    }

    let normalized_label = normalize_country_label(label);
    if matches!(
        normalized_label.as_str(),
        "america latina y el caribe espanol" | "latin america and the caribbean english"
    ) {
        return None;
    }

    if let Some(region) = REGIONS
        .iter()
        .find(|region| normalize_country_label(region.name) == normalized_label)
    {
        return Some(region.code.to_string());
    }

    let site = site_code.to_ascii_uppercase();
    if site.len() == 2 && site != "LA" && site.chars().all(|c| c.is_ascii_alphabetic()) {
        Some(site)
    } else {
        None
    }
}

fn country_code_from_language(language: Option<&str>) -> Option<String> {
    let language = language?;
    let parts = language.split(['-', '_']).skip(1).collect::<Vec<_>>();
    parts
        .iter()
        .rev()
        .find(|part| part.len() == 2 && part.chars().all(|c| c.is_ascii_alphabetic()))
        .map(|part| part.to_ascii_uppercase())
}

fn country_code_from_native_label(label: &str) -> Option<&'static str> {
    match label.trim() {
        "Việt Nam" => Some("VN"),
        "Россия" => Some("RU"),
        "中国大陆" => Some("CN"),
        "香港" => Some("HK"),
        "日本" => Some("JP"),
        "대한민국" => Some("KR"),
        "澳門" => Some("MO"),
        "台灣" => Some("TW"),
        "ไทย" => Some("TH"),
        "България" => Some("BG"),
        "Ελλάδα" => Some("GR"),
        "Україна" => Some("UA"),
        "البحرين" => Some("BH"),
        "مصر" => Some("EG"),
        "الأردن" => Some("JO"),
        "الكويت" => Some("KW"),
        "عُمان" => Some("OM"),
        "قطر" => Some("QA"),
        "المملكة العربية السعودية" => Some("SA"),
        "الإمارات العربية المتحدة" => Some("AE"),
        _ => None,
    }
}

fn normalize_country_label(label: &str) -> String {
    label
        .chars()
        .flat_map(char::to_lowercase)
        .map(|c| if c.is_alphanumeric() { c } else { ' ' })
        .collect::<String>()
        .split_whitespace()
        .collect::<Vec<_>>()
        .join(" ")
}

fn region_audit_messages(report: &RegionAuditReport, base_region: Region) -> Vec<String> {
    let mut messages = Vec::new();
    if let Some(aliases) = report.route_aliases_by_region.get(base_region.code) {
        let routes = aliases
            .iter()
            .map(|alias| format!("/{}/", alias.to_ascii_lowercase()))
            .collect::<Vec<_>>()
            .join(", ");
        messages.push(format!(
            "Region note: Apple.com routes {} through {}; App Store storefront code remains {}.",
            base_region.name, routes, base_region.code
        ));
    }

    if !report.missing_candidates.is_empty() {
        let candidates = report
            .missing_candidates
            .iter()
            .map(|candidate| format!("{} ({})", candidate.code, candidate.label))
            .collect::<Vec<_>>()
            .join(", ");
        messages.push(
            format!(
                "Region list notice: Apple currently lists possible App Store country codes not in this build: {candidates}."
            ),
        );
    }

    messages
}

fn default_region_for_currency(currency: &str) -> Region {
    let currency = currency.trim().to_ascii_uppercase();
    let preferred_code = match currency.as_str() {
        "USD" => "US",
        "EUR" => "DE",
        "GBP" => "GB",
        "SGD" => "SG",
        "AUD" => "AU",
        "CAD" => "CA",
        "JPY" => "JP",
        "CNY" => "CN",
        "HKD" => "HK",
        "KRW" => "KR",
        "TWD" => "TW",
        "INR" => "IN",
        "BRL" => "BR",
        "MXN" => "MX",
        "NZD" => "NZ",
        "MYR" => "MY",
        "PHP" => "PH",
        "THB" => "TH",
        "IDR" => "ID",
        "VND" => "VN",
        "CHF" => "CH",
        "SEK" => "SE",
        "NOK" => "NO",
        "DKK" => "DK",
        "PLN" => "PL",
        "TRY" => "TR",
        "ZAR" => "ZA",
        _ => REGIONS
            .iter()
            .find(|r| currency_for_region(r.code) == Some(currency.as_str()))
            .map(|r| r.code)
            .unwrap_or("US"),
    };

    REGIONS
        .iter()
        .copied()
        .find(|r| r.code == preferred_code)
        .unwrap_or(REGIONS[0])
}

fn currency_for_region(region_code: &str) -> Option<&'static str> {
    let code = region_code.to_ascii_uppercase();
    Some(match code.as_str() {
        "DZ" => "DZD",
        "AO" => "AOA",
        "BJ" | "BF" | "CI" | "GW" | "ML" | "NE" | "SN" => "XOF",
        "BH" => "BHD",
        "BW" => "BWP",
        "CM" | "CF" | "GQ" => "XAF",
        "CD" => "CDF",
        "EG" => "EGP",
        "GH" => "GHS",
        "GN" => "GNF",
        "IN" => "INR",
        "IL" => "ILS",
        "JO" => "JOD",
        "KE" => "KES",
        "KW" => "KWD",
        "LR" => "LRD",
        "LY" => "LYD",
        "MG" => "MGA",
        "MW" => "MWK",
        "MR" => "MRU",
        "MU" => "MUR",
        "MA" => "MAD",
        "MZ" => "MZN",
        "NA" => "NAD",
        "NG" => "NGN",
        "OM" => "OMR",
        "PK" => "PKR",
        "QA" => "QAR",
        "RW" => "RWF",
        "SA" => "SAR",
        "SC" => "SCR",
        "SL" => "SLL",
        "ZA" => "ZAR",
        "TZ" => "TZS",
        "TN" => "TND",
        "UG" => "UGX",
        "AE" => "AED",
        "ZM" => "ZMW",
        "ZW" => "USD",
        "AU" => "AUD",
        "BD" => "BDT",
        "BT" => "BTN",
        "BN" => "BND",
        "KH" => "KHR",
        "CN" => "CNY",
        "FJ" => "FJD",
        "HK" => "HKD",
        "ID" => "IDR",
        "JP" => "JPY",
        "KZ" => "KZT",
        "KG" => "KGS",
        "MO" => "MOP",
        "MY" => "MYR",
        "MV" => "MVR",
        "MN" => "MNT",
        "MM" => "MMK",
        "NP" => "NPR",
        "NZ" => "NZD",
        "PH" => "PHP",
        "SG" => "SGD",
        "KR" => "KRW",
        "LK" => "LKR",
        "TW" => "TWD",
        "TJ" => "TJS",
        "TH" => "THB",
        "TM" => "TMT",
        "UZ" => "UZS",
        "VN" => "VND",
        "AL" => "ALL",
        "AM" => "AMD",
        "AT" | "BE" | "CY" | "EE" | "FI" | "FR" | "DE" | "GR" | "IE" | "IT" | "XK" | "LV"
        | "LT" | "LU" | "MT" | "ME" | "NL" | "PT" | "SK" | "SI" | "ES" | "HR" => "EUR",
        "AZ" => "AZN",
        "BY" => "BYN",
        "BA" => "BAM",
        "BG" => "BGN",
        "CZ" => "CZK",
        "DK" => "DKK",
        "GE" => "GEL",
        "HU" => "HUF",
        "IS" => "ISK",
        "LI" | "CH" => "CHF",
        "MD" => "MDL",
        "MK" => "MKD",
        "NO" => "NOK",
        "PL" => "PLN",
        "RO" => "RON",
        "RU" => "RUB",
        "SE" => "SEK",
        "TR" => "TRY",
        "UA" => "UAH",
        "GB" => "GBP",
        "AI" | "AG" | "DM" | "GD" | "KN" | "LC" | "MS" | "VC" => "XCD",
        "AR" => "ARS",
        "BS" => "BSD",
        "BB" => "BBD",
        "BZ" => "BZD",
        "BM" => "BMD",
        "BO" => "BOB",
        "BR" => "BRL",
        "VG" => "USD",
        "KY" => "KYD",
        "CL" => "CLP",
        "CO" => "COP",
        "CR" => "CRC",
        "DO" => "DOP",
        "EC" => "USD",
        "SV" => "USD",
        "GT" => "GTQ",
        "GY" => "GYD",
        "HN" => "HNL",
        "JM" => "JMD",
        "MX" => "MXN",
        "NI" => "NIO",
        "PA" => "USD",
        "PY" => "PYG",
        "PE" => "PEN",
        "SR" => "SRD",
        "TT" => "TTD",
        "TC" => "USD",
        "UY" => "UYU",
        "VE" => "VES",
        "CA" => "CAD",
        "US" | "PR" | "FM" | "NR" => "USD",
        "PG" => "PGK",
        "SB" => "SBD",
        "TO" => "TOP",
        "VU" => "VUV",
        _ => return None,
    })
}

fn app_storefront_currency_for_region(region_code: &str) -> Option<&'static str> {
    let code = region_code.to_ascii_uppercase();
    Some(match code.as_str() {
        "AE" => "AED",
        "AU" => "AUD",
        "AT" | "BE" | "BA" | "BG" | "HR" | "CY" | "EE" | "FI" | "FR" | "DE" | "GR" | "IE"
        | "IT" | "XK" | "LV" | "LT" | "LU" | "MT" | "ME" | "NL" | "PT" | "SK" | "SI" | "ES" => {
            "EUR"
        }
        "BR" => "BRL",
        "CA" => "CAD",
        "CH" | "LI" => "CHF",
        "CL" => "CLP",
        "CN" => "CNY",
        "CO" => "COP",
        "CZ" => "CZK",
        "DK" => "DKK",
        "EG" => "EGP",
        "GB" => "GBP",
        "HK" => "HKD",
        "HU" => "HUF",
        "ID" => "IDR",
        "IL" => "ILS",
        "IN" => "INR",
        "JP" => "JPY",
        "KZ" => "KZT",
        "KR" => "KRW",
        "MX" => "MXN",
        "MY" => "MYR",
        "NG" => "NGN",
        "NO" => "NOK",
        "NZ" => "NZD",
        "PE" => "PEN",
        "PH" => "PHP",
        "PK" => "PKR",
        "PL" => "PLN",
        "QA" => "QAR",
        "RO" => "RON",
        "RU" => "RUB",
        "SA" => "SAR",
        "SE" => "SEK",
        "SG" => "SGD",
        "TH" => "THB",
        "TR" => "TRY",
        "TW" => "TWD",
        "TZ" => "TZS",
        "VN" => "VND",
        "ZA" => "ZAR",
        "US" => "USD",
        "AF" | "AI" | "AG" | "AL" | "AO" | "AR" | "AM" | "AZ" | "BS" | "BD" | "BB" | "BZ"
        | "BH" | "BJ" | "BM" | "BT" | "BO" | "BW" | "BN" | "BF" | "KH" | "CM" | "CF" | "CR"
        | "CI" | "CD" | "DM" | "DO" | "DZ" | "EC" | "SV" | "GQ" | "FJ" | "FM" | "GE" | "GH"
        | "GN" | "GD" | "GT" | "GW" | "GY" | "HN" | "IS" | "JM" | "JO" | "KE" | "KW" | "KG"
        | "LA" | "LR" | "LY" | "MO" | "MK" | "MG" | "MW" | "MV" | "ML" | "MR" | "MU" | "MD"
        | "MN" | "MA" | "MS" | "MM" | "MZ" | "NA" | "NR" | "NP" | "NI" | "NE" | "OM" | "PA"
        | "PG" | "PR" | "PY" | "RW" | "LC" | "KN" | "VC" | "SN" | "SC" | "SL" | "LK" | "SB"
        | "SR" | "TJ" | "TO" | "TT" | "TN" | "TM" | "TC" | "UG" | "UA" | "UY" | "UZ" | "VE"
        | "VU" | "VG" | "YE" | "ZM" | "ZW" => "USD",
        _ => return currency_for_region(&code),
    })
}

fn app_store_url(app_id: &str, region_code: &str) -> String {
    format!(
        "https://apps.apple.com/{}/app/id{}",
        region_code.to_lowercase(),
        app_id
    )
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct Pricing {
    region_code: String,
    region: String,
    amount: f64,
    currency: String,
    converted_amount: Option<f64>,
}

/// Map ISO currency code to symbol
fn currency_symbol(code: &str) -> &str {
    match code {
        // Africa, Middle East, India
        "DZD" => "دج",   // Algerian Dinar
        "AOA" => "Kz",   // Angolan Kwanza
        "XOF" => "Fr",   // CFA Franc BCEAO
        "BHD" => "BD",   // Bahraini Dinar
        "BWP" => "P",    // Botswana Pula
        "XAF" => "Fr",   // CFA Franc BEAC
        "CDF" => "FC",   // Congolese Franc
        "EGP" => "£",    // Egyptian Pound
        "GHS" => "₵",    // Ghanaian Cedi
        "GNF" => "FG",   // Guinean Franc
        "INR" => "₹",    // Indian Rupee
        "ILS" => "₪",    // Israeli New Shekel
        "JOD" => "JD",   // Jordanian Dinar
        "KES" => "KSh",  // Kenyan Shilling
        "KWD" => "KD",   // Kuwaiti Dinar
        "LRD" => "$",    // Liberian Dollar
        "LYD" => "LD",   // Libyan Dinar
        "MGA" => "Ar",   // Malagasy Ariary
        "MWK" => "MK",   // Malawian Kwacha
        "MRU" => "UM",   // Mauritanian Ouguiya
        "MUR" => "₨",    // Mauritian Rupee
        "MAD" => "د.م.", // Moroccan Dirham
        "MZN" => "MTn",  // Mozambican Metical
        "NAD" => "$",    // Namibian Dollar
        "NGN" => "₦",    // Nigerian Naira
        "OMR" => "ر.ع.", // Omani Rial
        "PKR" => "₨",    // Pakistani Rupee
        "QAR" => "ر.ق",  // Qatari Riyal
        "RWF" => "FRw",  // Rwandan Franc
        "SAR" => "ر.س",  // Saudi Riyal
        "SCR" => "SR",   // Seychellois Rupee
        "SLL" => "Le",   // Sierra Leonean Leone
        "ZAR" => "R",    // South African Rand
        "TZS" => "TZS",  // Tanzanian Shilling
        "TND" => "د.ت",  // Tunisian Dinar
        "UGX" => "USh",  // Ugandan Shilling
        "AED" => "د.إ",  // UAE Dirham
        "ZMW" => "ZK",   // Zambian Kwacha
        "ZWL" => "Z$",   // Zimbabwean Dollar
        // Asia Pacific
        "AUD" => "$",    // Australian Dollar
        "BDT" => "৳",    // Bangladeshi Taka
        "BTN" => "Nu.",  // Bhutanese Ngultrum
        "BND" => "B$",   // Brunei Dollar
        "KHR" => "៛",    // Cambodian Riel (suffix)
        "CNY" => "¥",    // Chinese Yuan
        "FJD" => "FJ$",  // Fiji Dollar
        "HKD" => "HK$",  // Hong Kong Dollar
        "IDR" => "Rp",   // Indonesian Rupiah
        "JPY" => "¥",    // Japanese Yen
        "KZT" => "₸",    // Kazakhstani Tenge
        "KGS" => "лв",   // Kyrgyzstani Som
        "MOP" => "P",    // Macanese Pataca
        "MYR" => "RM",   // Malaysian Ringgit
        "MVR" => "Rf.",  // Maldivian Rufiyaa
        "MNT" => "₮",    // Mongolian Tögrög
        "MMK" => "K",    // Myanmar Kyat (suffix)
        "NPR" => "₨",    // Nepalese Rupee
        "NZD" => "NZ$",  // New Zealand Dollar
        "PHP" => "₱",    // Philippine Peso
        "SGD" => "S$",   // Singapore Dollar
        "KRW" => "₩",    // South Korean Won
        "LKR" => "Rs",   // Sri Lankan Rupee
        "TWD" => "NT$",  // Taiwan Dollar
        "TJS" => "TJS",  // Tajikistani Somoni
        "THB" => "฿",    // Thai Baht
        "TMT" => "m",    // Turkmenistani Manat
        "UZS" => "so'm", // Uzbekistani so'm
        "VND" => "₫",    // Vietnamese Dong (suffix)
        // Europe
        "ALL" => "L",   // Albanian Lek (suffix)
        "AMD" => "AMD", // Armenian Dram
        "EUR" => "€",   // Euro
        "AZN" => "₼",   // Azerbaijani Manat
        "BYN" => "Br",  // Belarusian Ruble (suffix)
        "BAM" => "KM",  // Bosnia Convertible Mark
        "BGN" => "лв",  // Bulgarian Lev
        "HRK" => "kn",  // Croatian Kuna
        "CZK" => "Kč",  // Czech Koruna
        "DKK" => "kr",  // Danish Krone
        "GEL" => "₾",   // Georgian Lari
        "HUF" => "HUF", // Hungarian Forint (suffix; ISO)
        "ISK" => "kr",  // Icelandic Króna
        "MDL" => "L",   // Moldovan Leu (suffix)
        "MKD" => "ден", // North Macedonian Denar (suffix)
        "NOK" => "kr",  // Norwegian Krone
        "PLN" => "zł",  // Polish Zloty
        "RON" => "lei", // Romanian leu (suffix, decimals)
        "RUB" => "₽",   // Russian Ruble
        "SEK" => "kr",  // Swedish Krona
        "CHF" => "Fr.", // Swiss Franc
        "TRY" => "₺",   // Turkish Lira
        "UAH" => "₴",   // Ukrainian Hryvnia
        "GBP" => "£",   // British Pound
        // Latin America / Caribbean
        "XCD" => "EC$",  // Eastern Caribbean Dollar
        "ARS" => "$",    // Argentine Peso
        "BSD" => "B$",   // Bahamian Dollar
        "BBD" => "Bds$", // Barbadian Dollar
        "BZD" => "BZ$",  // Belize Dollar
        "BOB" => "Bs.",  // Bolivian Boliviano
        "BRL" => "R$",   // Brazilian Real
        "KYD" => "CI$",  // Cayman Islands Dollar
        "CLP" => "$",    // Chilean Peso
        "COP" => "$",    // Colombian Peso
        "CRC" => "₡",    // Costa Rican Colón
        "DOP" => "RD$",  // Dominican Peso
        "GTQ" => "Q",    // Guatemalan Quetzal
        "GYD" => "G$",   // Guyanese Dollar
        "HNL" => "L",    // Honduran Lempira
        "JMD" => "J$",   // Jamaican Dollar
        "MXN" => "$",    // Mexican Peso
        "NIO" => "C$",   // Nicaraguan Córdoba
        "PAB" => "B/.",  // Panamanian Balboa
        "PYG" => "₲",    // Paraguayan guaraní
        "PEN" => "S/.",  // Peruvian Sol
        "SRD" => "$",    // Surinamese dollar
        "TTD" => "TT$",  // Trinidad and Tobago Dollar
        "UYU" => "$",    // Uruguayan peso
        "VES" => "Bs.S", // Venezuelan Bolívar Soberano
        // North America
        "CAD" => "$", // Canadian Dollar
        "USD" => "$", // US Dollar
        // Oceania
        "PGK" => "K",   // Papua New Guinea Kina
        "SBD" => "SI$", // Solomon Islands dollar
        "TOP" => "T$",  // Tongan paʻanga (suffix)
        "VUV" => "VT",  // Vanuatu vatu (suffix)
        _ => "",
    }
}

fn currency_is_suffix(code: &str) -> bool {
    matches!(
        code,
        // Africa (suffix)
        "DZD" | "AOA" | "BWP" | "GHS" | "KES" | "LSL" | "LYD"
        | "MGA" | "MWK" | "MUR" | "MZN" | "NAD" | "NGN" | "RWF" | "SCR" | "SLL" | "SZL"
        | "TZS" | "UGX" | "XAF" | "XOF" | "ZAR" | "ZMW" | "ZWL" | "KMF" | "CFA" | "CDF"
        // Asia
        | "KHR" | "MMK" | "VND"
        // Oceania
        | "VUV" | "TOP"
        // Europe (suffix)
        | "ALL" | "MKD" | "MDL" | "RON" | "RSD" | "UAH" | "HUF" | "BYN"
    )
}

fn format_amount(amount: f64, code: &str) -> String {
    match code {
        // No decimals for these
        "JPY" | "KRW" | "VND" | "IDR" | "MMK" | "LAK" | "KHR" | "UGX" | "TZS" | "MWK" | "MGA"
        | "CDF" | "RWF" | "GNF" | "XOF" | "XAF" | "KMF" | "MZN" | "BIF" | "VUV" | "SLL" | "BYN" => {
            format!("{:.0}", amount)
        }
        // 3 decimals for some Gulf/Arab currencies
        "KWD" | "BHD" | "IQD" | "OMR" | "TND" | "LYD" | "JOD" => format!("{:.3}", amount),
        _ => format!("{:.2}", amount),
    }
}

fn format_price(amount: f64, code: &str) -> String {
    if amount <= 0.0 {
        return "Free".to_string();
    }

    let symbol = currency_symbol(code);
    if currency_is_suffix(code) {
        if !symbol.is_empty() {
            format!("{} {}", format_amount(amount, code), symbol)
        } else {
            format!("{} {}", format_amount(amount, code), code)
        }
    } else {
        if !symbol.is_empty() {
            format!("{}{}", symbol, format_amount(amount, code))
        } else {
            format!("{} {}", code, format_amount(amount, code))
        }
    }
}

async fn get_conversion_rate(client: &reqwest::Client, base: &str) -> Result<HashMap<String, f64>> {
    let url = format!("https://open.er-api.com/v6/latest/{}", base);
    let res = fetch_json(client, &url).await?;
    if res.get("result").and_then(Value::as_str) == Some("error") {
        return Err(anyhow!("Exchange-rate API rejected base currency {base}"));
    }

    let rates = res["rates"]
        .as_object()
        .context("Missing exchange rates in response")?
        .iter()
        .filter_map(|(k, v)| v.as_f64().map(|f| (k.clone(), f)))
        .collect();
    Ok(rates)
}

/// Build HTTP client with proper headers to avoid blocking
fn build_http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .user_agent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15")
        .timeout(Duration::from_secs(HTTP_TIMEOUT_SECS))
        .connect_timeout(Duration::from_secs(HTTP_CONNECT_TIMEOUT_SECS))
        .pool_idle_timeout(Duration::from_secs(90))
        .pool_max_idle_per_host(HTTP_POOL_MAX_IDLE_PER_HOST)
        .build()
        .context("Failed to build HTTP client")
}

async fn fetch_text(client: &reqwest::Client, url: &str) -> Result<String> {
    Ok(client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .text()
        .await?)
}

async fn fetch_json(client: &reqwest::Client, url: &str) -> Result<Value> {
    Ok(client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .json::<Value>()
        .await?)
}

fn og_title_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r#"<meta property="og:title" content="([^"]+)""#).expect("valid og:title regex")
    })
}

fn shoebox_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r#"<script[^>]*id="shoebox-media-api-cache-apps"[^>]*>([\s\S]*?)</script>"#)
            .expect("valid shoebox regex")
    })
}

fn text_pair_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r#"<div class="text-pair[^"]*"><span>([^<]+)</span>\s*<span>([^<]+)</span>"#)
            .expect("valid IAP text-pair regex")
    })
}

fn iap_section_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r#"<dt[^>]*>\s*In-App Purchases\s*</dt>\s*<dd[^>]*>([\s\S]*?)</dd>"#)
            .expect("valid IAP section regex")
    })
}

fn serialized_server_data_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r#"<script[^>]*id="serialized-server-data"[^>]*>([\s\S]*?)</script>"#)
            .expect("valid serialized-server-data regex")
    })
}

fn dangling_iap_parenthetical_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\s*\([^)]*$").expect("valid dangling IAP regex"))
}

fn og_price_amount_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r#"<meta property="og:price:amount" content="([^"]+)""#)
            .expect("valid og price amount regex")
    })
}

fn og_price_currency_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r#"<meta property="og:price:currency" content="([^"]+)""#)
            .expect("valid og price currency regex")
    })
}

fn json_ld_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| {
        Regex::new(r#"<script[^>]*type="application/ld\+json"[^>]*>([\s\S]*?)</script>"#)
            .expect("valid JSON-LD regex")
    })
}

fn iso_currency_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\b([A-Z]{3})\b").expect("valid ISO currency regex"))
}

fn idr_thousand_unit_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"(?i)\d[\d\s.,]*(?:ribu|rb)\b").expect("valid IDR unit regex"))
}

async fn fetch_app_name(
    client: &reqwest::Client,
    app_id: &str,
    region_code: &str,
) -> Option<String> {
    let url = app_store_url(app_id, region_code);
    let html = fetch_text(client, &url).await.ok()?;
    let caps = og_title_regex().captures(&html)?;
    let title = caps.get(1)?.as_str();
    Some(decode_html_entities(title).trim().to_string())
}

fn parse_app_data_from_html(html: &str) -> Result<Value> {
    let caps = shoebox_regex()
        .captures(html)
        .context("Unable to find App Store cache script in HTML")?;
    let raw = caps[1].trim();
    let outer: Value = serde_json::from_str(raw)?;
    if let Some(map) = outer.as_object() {
        // Prefer entries with IAP metadata.
        for v in map.values() {
            if let Some(s) = v.as_str() {
                if let Some(app_data) = parse_shoebox_app_data(s) {
                    if app_data
                        .pointer("/relationships/top-in-apps/data")
                        .is_some_and(Value::is_array)
                    {
                        return Ok(app_data);
                    }
                }
            }
        }

        // Fallback
        for v in map.values() {
            if let Some(s) = v.as_str() {
                if let Some(app_data) = parse_shoebox_app_data(s) {
                    return Ok(app_data);
                }
            }
        }
    }
    Err(anyhow!("Failed to extract App Store JSON data"))
}

fn parse_shoebox_app_data(raw: &str) -> Option<Value> {
    let val = serde_json::from_str::<Value>(raw).ok()?;
    val.get("d")?.as_array()?.first().cloned()
}

#[derive(Debug, Clone)]
struct IAPItem {
    name: String,
    price_string: String,
}

fn display_iap_name(name: &str) -> String {
    let trimmed = name.trim();
    let cleaned = dangling_iap_parenthetical_regex()
        .replace(trimmed, "")
        .trim()
        .to_string();

    if !cleaned.is_empty() && cleaned.len() < trimmed.len() {
        format!("{cleaned}...")
    } else {
        trimmed.to_string()
    }
}

/// Fetch IAP list from the App Store HTML page.
async fn fetch_iap_list_from_html(
    client: &reqwest::Client,
    app_id: &str,
    region_code: &str,
) -> Result<Vec<IAPItem>> {
    let url = app_store_url(app_id, region_code);
    let html = fetch_text(client, &url).await?;
    Ok(parse_iap_list_from_html(&html))
}

fn parse_iap_list_from_html(html: &str) -> Vec<IAPItem> {
    let iaps = parse_iap_list_from_serialized_server_data(html);
    if !iaps.is_empty() {
        return iaps;
    }

    parse_iap_list_from_visible_html(html)
}

fn parse_iap_list_from_serialized_server_data(html: &str) -> Vec<IAPItem> {
    let Some(raw) = serialized_server_data_regex()
        .captures(html)
        .and_then(|caps| caps.get(1).map(|m| m.as_str()))
    else {
        return Vec::new();
    };

    let Ok(root) = serde_json::from_str::<Value>(raw) else {
        return Vec::new();
    };

    let Some(items) = root
        .pointer("/data/0/data/shelfMapping/information/items")
        .and_then(Value::as_array)
    else {
        return Vec::new();
    };

    let mut iaps = Vec::new();
    for item in items {
        if item.get("title").and_then(Value::as_str) != Some("In-App Purchases") {
            continue;
        }

        collect_iap_text_pairs_from_value(item, &mut iaps);
        break;
    }

    dedup_iap_rows(iaps)
}

fn collect_iap_text_pairs_from_value(value: &Value, iaps: &mut Vec<IAPItem>) {
    match value {
        Value::Array(values) => {
            if let Some((name, price)) = parse_iap_text_pair_array(values) {
                push_iap_row(iaps, name, price);
            } else {
                for value in values {
                    collect_iap_text_pairs_from_value(value, iaps);
                }
            }
        }
        Value::Object(map) => {
            if map.get("$kind").and_then(Value::as_str) == Some("textPair") {
                if let (Some(name), Some(price)) = (
                    map.get("leadingText").and_then(Value::as_str),
                    map.get("trailingText").and_then(Value::as_str),
                ) {
                    push_iap_row(iaps, name, price);
                }
            }

            for value in map.values() {
                collect_iap_text_pairs_from_value(value, iaps);
            }
        }
        _ => {}
    }
}

fn parse_iap_text_pair_array(values: &[Value]) -> Option<(&str, &str)> {
    if values.len() != 2 {
        return None;
    }

    Some((values[0].as_str()?, values[1].as_str()?))
}

fn parse_iap_list_from_visible_html(html: &str) -> Vec<IAPItem> {
    let mut iaps = Vec::new();
    let iap_section = iap_section_regex()
        .captures(html)
        .and_then(|caps| caps.get(1).map(|section| section.as_str()))
        .unwrap_or(html);

    // Parse IAPs from HTML: <div class="text-pair"><span>NAME</span> <span>PRICE</span>
    for cap in text_pair_regex().captures_iter(iap_section) {
        let name = decode_html_entities(cap[1].trim()).to_string();
        let price = decode_html_entities(cap[2].trim()).to_string();

        // Skip if it is not an IAP price row.
        if price.is_empty() || !price.chars().any(|c| c.is_ascii_digit()) {
            continue;
        }

        push_iap_row(&mut iaps, &name, &price);
    }

    dedup_iap_rows(iaps)
}

fn push_iap_row(iaps: &mut Vec<IAPItem>, name: &str, price: &str) {
    let name = decode_html_entities(name.trim()).to_string();
    let price = decode_html_entities(price.trim()).to_string();

    if name.is_empty() || price.is_empty() || !price.chars().any(|c| c.is_ascii_digit()) {
        return;
    }

    iaps.push(IAPItem {
        name,
        price_string: price,
    });
}

fn dedup_iap_rows(iaps: Vec<IAPItem>) -> Vec<IAPItem> {
    let mut seen = std::collections::HashSet::new();
    iaps.into_iter()
        .filter(|iap| seen.insert((iap.name.clone(), iap.price_string.clone())))
        .collect()
}

fn parse_price_from_string_for_currency(price_str: &str, currency: &str) -> Option<f64> {
    parse_price_from_string_with_currency(price_str, Some(currency))
}

/// Parse price from formatted string (e.g., "$11.99" -> 11.99).
fn parse_price_from_string_with_currency(price_str: &str, currency: Option<&str>) -> Option<f64> {
    let cleaned: String = price_str
        .chars()
        .filter(|c| c.is_ascii_digit() || *c == '.' || *c == ',')
        .collect();

    if cleaned.is_empty() {
        return None;
    }

    let amount = normalize_price_number(&cleaned, currency)
        .parse::<f64>()
        .ok()?;

    Some(amount * localized_price_multiplier(price_str, currency))
}

fn normalize_price_number(cleaned: &str, currency: Option<&str>) -> String {
    let dot = cleaned.rfind('.');
    let comma = cleaned.rfind(',');

    match (dot, comma) {
        (Some(dot_pos), Some(comma_pos)) => {
            let decimal_pos = dot_pos.max(comma_pos);
            cleaned
                .char_indices()
                .filter_map(|(idx, c)| {
                    if c.is_ascii_digit() {
                        Some(c)
                    } else if idx == decimal_pos {
                        Some('.')
                    } else {
                        None
                    }
                })
                .collect()
        }
        (Some(pos), None) => normalize_single_separator(cleaned, '.', pos, currency),
        (None, Some(pos)) => normalize_single_separator(cleaned, ',', pos, currency),
        (None, None) => cleaned.to_string(),
    }
}

fn normalize_single_separator(
    cleaned: &str,
    separator: char,
    pos: usize,
    currency: Option<&str>,
) -> String {
    let occurrences = cleaned.matches(separator).count();
    let digits_after = cleaned.len().saturating_sub(pos + 1);

    if currency.is_some_and(|code| currency_uses_three_decimals(code) && digits_after == 3) {
        return if separator == ',' {
            cleaned.replace(',', ".")
        } else {
            cleaned.to_string()
        };
    }

    if occurrences > 1 || digits_after == 0 || digits_after == 3 {
        cleaned.chars().filter(|c| c.is_ascii_digit()).collect()
    } else if separator == ',' {
        cleaned.replace(',', ".")
    } else {
        cleaned.to_string()
    }
}

fn currency_uses_three_decimals(code: &str) -> bool {
    matches!(code, "KWD" | "BHD" | "IQD" | "OMR" | "TND" | "LYD" | "JOD")
}

fn localized_price_multiplier(price_str: &str, currency: Option<&str>) -> f64 {
    if currency.is_some_and(|code| code.eq_ignore_ascii_case("IDR"))
        && idr_thousand_unit_regex().is_match(price_str)
    {
        1000.0
    } else {
        1.0
    }
}

fn currency_for_bare_dollar(region_code: &str) -> &'static str {
    app_storefront_currency_for_region(region_code).unwrap_or("USD")
}

fn normalize_display_currency_code(code: &str) -> String {
    match code {
        "FRW" => "RWF",
        "KSH" => "KES",
        "LEI" => "RON",
        "TSH" => "TZS",
        "USH" => "UGX",
        _ => code,
    }
    .to_string()
}

/// Detect currency from price string
fn detect_currency_from_string(price_str: &str, region_code: &str) -> String {
    let price = price_str.trim();
    let upper = price.to_ascii_uppercase();

    if upper.contains("USD") || upper.contains("US$") || upper.contains("$US") {
        "USD".to_string()
    } else if price.contains('৳') || upper.contains("BDT") {
        "BDT".to_string()
    } else if upper.contains("HK$") || upper.contains("HKD") {
        "HKD".to_string()
    } else if upper.contains("NZ$") || upper.contains("NZD") {
        "NZD".to_string()
    } else if upper.contains("CA$") || upper.contains("C$") || upper.contains("CAD") {
        "CAD".to_string()
    } else if upper.contains("AU$") || upper.contains("A$") || upper.contains("AUD") {
        "AUD".to_string()
    } else if upper.contains("SG$") || upper.contains("S$") || upper.contains("SGD") {
        "SGD".to_string()
    } else if upper.contains("R$") || upper.contains("BRL") {
        "BRL".to_string()
    } else if price.contains('$') {
        currency_for_bare_dollar(region_code).to_string()
    } else if price.contains('€') || upper.contains("EUR") {
        "EUR".to_string()
    } else if upper.contains("EGP")
        || (price.contains('£') && app_storefront_currency_for_region(region_code) == Some("EGP"))
    {
        "EGP".to_string()
    } else if price.contains('£') || upper.contains("GBP") {
        "GBP".to_string()
    } else if price.contains('¥') {
        match app_storefront_currency_for_region(region_code) {
            Some("CNY") => "CNY".to_string(),
            _ => "JPY".to_string(),
        }
    } else if upper.contains("AED") || price.contains("د.إ") {
        "AED".to_string()
    } else if upper.contains("QAR") || price.contains("ر.ق") {
        "QAR".to_string()
    } else if upper.contains("SAR") || price.contains("ر.س") {
        "SAR".to_string()
    } else if upper.contains("OMR") || price.contains("ر.ع") {
        "OMR".to_string()
    } else if upper.contains("KWD") || upper.contains("KD") {
        "KWD".to_string()
    } else if upper.contains("JOD") || upper.contains("JD") {
        "JOD".to_string()
    } else if upper.contains("MAD") || price.contains("د.م") {
        "MAD".to_string()
    } else if upper.contains("TND") || price.contains("د.ت") {
        "TND".to_string()
    } else if price.contains('₹') || upper.contains("INR") {
        "INR".to_string()
    } else if price.contains('₩') || upper.contains("KRW") {
        "KRW".to_string()
    } else if upper.contains("NT$") || upper.contains("TWD") {
        "TWD".to_string()
    } else if upper.contains("RM") || upper.contains("MYR") {
        "MYR".to_string()
    } else if price.contains('₱') || upper.contains("PHP") {
        "PHP".to_string()
    } else if price.contains('฿') || upper.contains("THB") {
        "THB".to_string()
    } else if price.contains('₫') || upper.contains("VND") {
        "VND".to_string()
    } else if upper.contains("RP") || upper.contains("IDR") {
        "IDR".to_string()
    } else if upper.contains("LEI") || upper.contains("RON") {
        "RON".to_string()
    } else if price.contains('₺') || upper.contains("TRY") || upper.contains("TL") {
        "TRY".to_string()
    } else if let Some(code) = iso_currency_regex()
        .captures(&upper)
        .and_then(|caps| caps.get(1))
        .map(|code| normalize_display_currency_code(code.as_str()))
    {
        code
    } else {
        app_storefront_currency_for_region(region_code)
            .unwrap_or("USD")
            .to_string()
    }
}

fn pricing_entry(region: Region, amount: f64, currency: &str) -> Option<Pricing> {
    Some(Pricing {
        region_code: region.code.to_string(),
        region: region.name.to_string(),
        amount,
        currency: currency.to_string(),
        converted_amount: None,
    })
}

fn json_number_as_f64(value: &Value) -> Option<f64> {
    value
        .as_f64()
        .or_else(|| value.as_str().and_then(|s| s.parse::<f64>().ok()))
}

fn extract_offer_price(offers: &Value) -> Option<(f64, String)> {
    let offer = offers
        .as_array()
        .and_then(|items| items.first())
        .unwrap_or(offers);
    let amount = offer.get("price").and_then(json_number_as_f64)?;
    let currency = offer.get("priceCurrency")?.as_str()?.to_string();
    Some((amount, currency))
}

async fn collect_base_app_pricing(
    client: &reqwest::Client,
    app_id: &str,
    region: Region,
) -> Option<Pricing> {
    // PRIMARY METHOD: iTunes Search API (most reliable!)
    let itunes_url = format!(
        "https://itunes.apple.com/lookup?id={}&country={}",
        app_id, region.code
    );

    if let Ok(json) = fetch_json(client, &itunes_url).await {
        let result = json["results"]
            .as_array()
            .and_then(|results| results.first())?;

        if let (Some(amount), Some(curr)) = (result["price"].as_f64(), result["currency"].as_str())
        {
            return pricing_entry(region, amount, curr);
        }

        return None;
    }

    let url = app_store_url(app_id, region.code);
    let html = match fetch_text(client, &url).await {
        Ok(html) => html,
        Err(_) => return None,
    };

    // FALLBACK 1: Try shoebox JSON from the already-fetched HTML.
    if let Ok(app_data) = parse_app_data_from_html(&html) {
        let attr = &app_data["attributes"];
        if let (Some(amount), Some(curr)) = (
            attr.get("price").and_then(|v| v.as_f64()),
            attr.get("currencyCode").and_then(|v| v.as_str()),
        ) {
            return pricing_entry(region, amount, curr);
        }
    }

    // FALLBACK 2: HTML scraping
    if let (Some(am), Some(cur)) = (
        og_price_amount_regex().captures(&html),
        og_price_currency_regex().captures(&html),
    ) {
        let amount_str = am.get(1).unwrap().as_str();
        if let Ok(amount) = amount_str.parse::<f64>() {
            let curr = cur.get(1).unwrap().as_str();
            return pricing_entry(region, amount, curr);
        }
    }

    if let Some(c) = json_ld_regex().captures(&html) {
        let blob = c[1].trim();
        if let Ok(val) = serde_json::from_str::<Value>(blob) {
            if let Some((amount, curr)) = val.get("offers").and_then(extract_offer_price) {
                return pricing_entry(region, amount, &curr);
            }
        }
    }

    None
}

async fn collect_iap_pricing_with_cache(
    client: &reqwest::Client,
    app_id: &str,
    region: Region,
    selected_name: &str,
    cached_iaps: Option<Vec<IAPItem>>,
) -> (Option<Pricing>, Option<(String, Vec<IAPItem>)>) {
    let (region_iaps, cache_update) = match cached_iaps {
        Some(iaps) => (iaps, None),
        None => {
            let Ok(iaps) = fetch_iap_list_from_html(client, app_id, region.code).await else {
                return (None, None);
            };
            (iaps.clone(), Some((region.code.to_string(), iaps)))
        }
    };

    let iap = region_iaps
        .into_iter()
        .find(|iap| iap.name == selected_name);
    let pricing = iap.and_then(|iap| {
        let currency = detect_currency_from_string(&iap.price_string, region.code);
        let amount = parse_price_from_string_for_currency(&iap.price_string, &currency)?;
        pricing_entry(region, amount, &currency)
    });

    (pricing, cache_update)
}

fn convert_prices(pricing: &mut [Pricing], rates: &HashMap<String, f64>) {
    for entry in pricing.iter_mut() {
        convert_price(entry, rates);
    }
    sort_pricing_by_converted(pricing);
}

fn convert_price(entry: &mut Pricing, rates: &HashMap<String, f64>) {
    if let Some(rate) = rates.get(&entry.currency) {
        entry.converted_amount = Some((entry.amount / rate * 100.0).round() / 100.0);
    }
}

fn sort_pricing_by_converted(pricing: &mut [Pricing]) {
    pricing.sort_by(|a, b| match (a.converted_amount, b.converted_amount) {
        (Some(a), Some(b)) => a.total_cmp(&b),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => a.region.cmp(&b.region),
    });
}

fn merge_pricing_entries(target: &mut Vec<Pricing>, updates: Vec<Pricing>) {
    for update in updates {
        if let Some(existing) = target
            .iter_mut()
            .find(|entry| entry.region_code == update.region_code)
        {
            *existing = update;
        } else {
            target.push(update);
        }
    }

    sort_pricing_by_converted(target);
}

#[derive(Clone)]
struct AnalysisResult {
    app_id: String,
    display_name: String,
    base_region: Region,
    base_currency: String,
    base_pricing: Vec<Pricing>,
    iaps: Vec<IAPItem>,
}

#[derive(Clone)]
struct IapPricingResult {
    name: String,
    pricing: Vec<Pricing>,
    region_iap_cache: HashMap<String, Vec<IAPItem>>,
}

enum WorkerMessage {
    Audit(Result<RegionAuditReport, String>),
    Analysis(Result<AnalysisResult, String>),
    IapCacheBatch {
        app_id: String,
        region_iap_cache: HashMap<String, Vec<IAPItem>>,
    },
    IapPricingProgress {
        app_id: String,
        request_name: String,
        checked: usize,
        total: usize,
        pricing: Vec<Pricing>,
        region_iap_cache: HashMap<String, Vec<IAPItem>>,
    },
    IapPricing {
        request_name: String,
        result: Result<IapPricingResult, String>,
    },
}

#[derive(Copy, Clone, PartialEq, Eq)]
enum PricingSort {
    Region,
    Price,
    Currency,
    Converted,
}

struct PricingTableState {
    filter: String,
    sort: PricingSort,
    ascending: bool,
}

impl PricingTableState {
    fn new(sort: PricingSort, ascending: bool) -> Self {
        Self {
            filter: String::new(),
            sort,
            ascending,
        }
    }

    fn toggle_sort(&mut self, sort: PricingSort) {
        if self.sort == sort {
            self.ascending = !self.ascending;
        } else {
            self.sort = sort;
            self.ascending = true;
        }
    }
}

struct PricingGui {
    runtime: Runtime,
    client: reqwest::Client,
    tx: Sender<WorkerMessage>,
    rx: Receiver<WorkerMessage>,
    app_input: String,
    base_currency: String,
    status: String,
    analysis: Option<AnalysisResult>,
    iap_result: Option<IapPricingResult>,
    audit_report: Option<RegionAuditReport>,
    selected_iap: Option<usize>,
    iap_region_cache: HashMap<String, Vec<IAPItem>>,
    base_table: PricingTableState,
    iap_table: PricingTableState,
    iap_filter: String,
    loading_analysis: bool,
    loading_iap: bool,
    active_iap_request: Option<String>,
}

impl PricingGui {
    fn new(cc: &eframe::CreationContext<'_>) -> Self {
        install_system_currency_font(&cc.egui_ctx);
        apply_gui_style(&cc.egui_ctx);

        let runtime = RuntimeBuilder::new_multi_thread()
            .worker_threads(TOKIO_WORKER_THREADS)
            .thread_name("appstore-pricing-worker")
            .enable_all()
            .build()
            .expect("Tokio runtime should initialize");
        let client = build_http_client().expect("HTTP client should initialize");
        let (tx, rx) = mpsc::channel();

        let app = Self {
            runtime,
            client,
            tx,
            rx,
            app_input: String::new(),
            base_currency: "SGD".to_string(),
            status: "Ready".to_string(),
            analysis: None,
            iap_result: None,
            audit_report: None,
            selected_iap: None,
            iap_region_cache: HashMap::new(),
            base_table: PricingTableState::new(PricingSort::Converted, true),
            iap_table: PricingTableState::new(PricingSort::Converted, true),
            iap_filter: String::new(),
            loading_analysis: false,
            loading_iap: false,
            active_iap_request: None,
        };

        app.spawn_region_audit();
        app
    }

    fn spawn_region_audit(&self) {
        let client = self.client.clone();
        let tx = self.tx.clone();
        self.runtime.spawn(async move {
            let result = audit_apple_country_regions(&client)
                .await
                .map_err(|error| error.to_string());
            let _ = tx.send(WorkerMessage::Audit(result));
        });
    }

    fn start_iap_cache_prefetch(&self, analysis: &AnalysisResult) {
        if analysis.iaps.is_empty() {
            return;
        }

        let client = self.client.clone();
        let tx = self.tx.clone();
        let analysis = analysis.clone();
        let cached_region_iaps = self.iap_region_cache.clone();
        self.runtime.spawn(async move {
            prefetch_iap_region_cache(client, analysis, cached_region_iaps, tx).await;
        });
    }

    fn receive_worker_messages(&mut self, ctx: &egui::Context) {
        while let Ok(message) = self.rx.try_recv() {
            match message {
                WorkerMessage::Audit(Ok(report)) => {
                    self.audit_report = Some(report);
                }
                WorkerMessage::Audit(Err(_)) => {}
                WorkerMessage::Analysis(Ok(result)) => {
                    self.loading_analysis = false;
                    self.loading_iap = false;
                    self.active_iap_request = None;
                    self.iap_region_cache.clear();
                    self.iap_region_cache
                        .insert(result.base_region.code.to_string(), result.iaps.clone());
                    self.status = format!(
                        "{} regions loaded, {} IAP rows found",
                        result.base_pricing.len(),
                        result.iaps.len()
                    );
                    self.selected_iap = (!result.iaps.is_empty()).then_some(0);
                    self.iap_result = None;
                    self.analysis = Some(result.clone());
                    self.start_iap_cache_prefetch(&result);
                }
                WorkerMessage::Analysis(Err(error)) => {
                    self.loading_analysis = false;
                    self.loading_iap = false;
                    self.active_iap_request = None;
                    self.status = error;
                    self.analysis = None;
                    self.iap_result = None;
                    self.iap_region_cache.clear();
                }
                WorkerMessage::IapCacheBatch {
                    app_id,
                    region_iap_cache,
                } => {
                    if self
                        .analysis
                        .as_ref()
                        .is_some_and(|analysis| analysis.app_id == app_id)
                    {
                        self.iap_region_cache.extend(region_iap_cache);
                    }
                }
                WorkerMessage::IapPricingProgress {
                    app_id,
                    request_name,
                    checked,
                    total,
                    pricing,
                    region_iap_cache,
                } => {
                    let same_app = self
                        .analysis
                        .as_ref()
                        .is_some_and(|analysis| analysis.app_id == app_id);
                    if same_app {
                        self.iap_region_cache.extend(region_iap_cache.clone());
                    }

                    if self.active_iap_request.as_deref() != Some(request_name.as_str()) {
                        continue;
                    }

                    let mut region_iap_cache = region_iap_cache;
                    let result = self.iap_result.get_or_insert_with(|| IapPricingResult {
                        name: request_name.clone(),
                        pricing: Vec::new(),
                        region_iap_cache: HashMap::new(),
                    });
                    result
                        .region_iap_cache
                        .extend(std::mem::take(&mut region_iap_cache));
                    merge_pricing_entries(&mut result.pricing, pricing);
                    self.status = format!(
                        "{} prices loaded, {}/{} regions checked for {}",
                        result.pricing.len(),
                        checked,
                        total,
                        display_iap_name(&request_name)
                    );
                }
                WorkerMessage::IapPricing {
                    request_name,
                    result,
                } => {
                    if self.active_iap_request.as_deref() != Some(request_name.as_str()) {
                        continue;
                    }

                    self.loading_iap = false;
                    self.active_iap_request = None;
                    match result {
                        Ok(mut result) => {
                            self.iap_region_cache
                                .extend(std::mem::take(&mut result.region_iap_cache));
                            self.status = format!(
                                "{} regions loaded for {}",
                                result.pricing.len(),
                                display_iap_name(&result.name)
                            );
                            self.iap_result = Some(result);
                        }
                        Err(error) => {
                            self.status = error;
                        }
                    }
                }
            }
            ctx.request_repaint();
        }
    }

    fn start_analysis(&mut self, ctx: &egui::Context) {
        let app_input = self.app_input.trim().to_string();
        if extract_app_id(&app_input).is_none() {
            self.status = "Enter a valid App Store URL or numeric App ID.".to_string();
            return;
        }

        let base_currency = self.base_currency.trim().to_ascii_uppercase();
        if base_currency.len() != 3 || !base_currency.chars().all(|c| c.is_ascii_alphabetic()) {
            self.status = "Enter a three-letter base currency code.".to_string();
            return;
        }

        self.base_currency = base_currency.clone();
        self.loading_analysis = true;
        self.loading_iap = false;
        self.active_iap_request = None;
        self.status = "Analyzing app pricing...".to_string();
        self.analysis = None;
        self.iap_result = None;
        self.iap_region_cache.clear();
        self.base_table.filter.clear();
        self.base_table.sort = PricingSort::Converted;
        self.base_table.ascending = true;
        self.iap_table.filter.clear();
        self.iap_table.sort = PricingSort::Converted;
        self.iap_table.ascending = true;
        self.iap_filter.clear();

        let client = self.client.clone();
        let tx = self.tx.clone();
        self.runtime.spawn(async move {
            let result = analyze_app_pricing(client, app_input, base_currency)
                .await
                .map_err(|error| error.to_string());
            let _ = tx.send(WorkerMessage::Analysis(result));
        });
        ctx.request_repaint();
    }

    fn start_iap_pricing(&mut self, ctx: &egui::Context) {
        let Some(analysis) = self.analysis.clone() else {
            return;
        };
        let Some(selected_index) = self.selected_iap else {
            self.status = "Select an in-app purchase first.".to_string();
            return;
        };
        let Some(iap) = analysis.iaps.get(selected_index).cloned() else {
            self.status = "Selected in-app purchase is unavailable.".to_string();
            return;
        };

        if self.loading_iap && self.active_iap_request.as_deref() == Some(iap.name.as_str()) {
            return;
        }

        self.loading_iap = true;
        self.active_iap_request = Some(iap.name.clone());
        self.status = format!("Checking {} across regions...", display_iap_name(&iap.name));
        self.iap_result = None;
        self.iap_table.filter.clear();
        self.iap_table.sort = PricingSort::Converted;
        self.iap_table.ascending = true;

        let client = self.client.clone();
        let tx = self.tx.clone();
        let iap_region_cache = self.iap_region_cache.clone();
        self.runtime.spawn(async move {
            let request_name = iap.name;
            let result = analyze_iap_pricing(
                client,
                analysis,
                request_name.clone(),
                iap_region_cache,
                tx.clone(),
            )
            .await
            .map_err(|error| error.to_string());
            let _ = tx.send(WorkerMessage::IapPricing {
                request_name,
                result,
            });
        });
        ctx.request_repaint();
    }

    fn draw_input_panel(&mut self, ui: &mut egui::Ui, ctx: &egui::Context) {
        toolbar_frame().show(ui, |ui| {
            ui.vertical(|ui| {
                ui.horizontal(|ui| {
                    ui.spacing_mut().item_spacing.x = 0.0;
                    ui.label(compact_field_label("App"));
                    ui.add_space(8.0);
                    let button_width = 118.0;
                    let action_gap = 12.0;
                    let url_width = (ui.available_width() - button_width - action_gap).max(280.0);
                    ui.add_sized(
                        [url_width, CONTROL_HEIGHT],
                        egui::TextEdit::singleline(&mut self.app_input)
                            .hint_text("apps.apple.com link or App ID"),
                    );
                    ui.add_space(action_gap);
                    let button = primary_button(if self.loading_analysis {
                        "..."
                    } else {
                        "Check Prices"
                    });
                    let clicked = ui
                        .add_enabled_ui(!self.loading_analysis, |ui| {
                            ui.add_sized([button_width, CONTROL_HEIGHT], button)
                                .clicked()
                        })
                        .inner;
                    if clicked {
                        self.start_analysis(ctx);
                    }
                });

                ui.add_space(3.0);
                let color = if self.loading_analysis || self.loading_iap {
                    accent_color()
                } else {
                    mut_text_color()
                };
                ui.horizontal(|ui| {
                    ui.spacing_mut().item_spacing.x = 0.0;
                    let row_width = ui.available_width();
                    let currency_row_height = 28.0;
                    let currency_group_width = 184.0;
                    let side_width = ((row_width - currency_group_width) / 2.0).max(0.0);
                    ui.add_sized(
                        [side_width, currency_row_height],
                        egui::Label::new(RichText::new(&self.status).size(12.0).color(color))
                            .truncate(),
                    );
                    ui.label(compact_field_label("Base currency"));
                    ui.add_space(10.0);
                    ui.add_sized(
                        [66.0, currency_row_height],
                        egui::TextEdit::singleline(&mut self.base_currency)
                            .char_limit(3)
                            .horizontal_align(egui::Align::Center)
                            .vertical_align(egui::Align::Center)
                            .hint_text("SGD"),
                    );
                    ui.add_sized([side_width, currency_row_height], egui::Label::new(""));
                });
            });
        });

        if ui.input(|input| input.key_pressed(egui::Key::Enter)) && !self.loading_analysis {
            self.start_analysis(ctx);
        }
    }

    fn draw_results(&mut self, ui: &mut egui::Ui, ctx: &egui::Context) {
        let Some(analysis) = self.analysis.clone() else {
            self.draw_empty_state(ui);
            return;
        };

        self.draw_metadata_cards(ui, &analysis);

        if let Some(report) = &self.audit_report {
            for message in region_audit_messages(report, analysis.base_region) {
                ui.add_space(5.0);
                notice_frame().show(ui, |ui| {
                    ui.label(RichText::new(message).size(11.0).color(warn_text_color()));
                });
            }
        }

        ui.add_space(6.0);
        main_workspace_frame().show(ui, |ui| {
            let edge_guard = 16.0;
            let gap = 8.0;
            let available = (ui.available_width() - edge_guard * 2.0 - gap * 2.0).max(0.0);
            let min_base = 320.0;
            let min_selector = 430.0;
            let min_detail = 320.0;
            let mut selector_width = (available * 0.38).clamp(min_selector, 500.0);
            let mut base_width = (available * 0.3).clamp(min_base, 460.0);
            let mut detail_width = available - base_width - selector_width;
            if detail_width < min_detail {
                let deficit = min_detail - detail_width;
                let base_reduction = (base_width - min_base).max(0.0).min(deficit * 0.6);
                base_width -= base_reduction;
                let selector_reduction = (selector_width - min_selector)
                    .max(0.0)
                    .min(deficit - base_reduction);
                selector_width -= selector_reduction;
                detail_width = (available - base_width - selector_width).max(0.0);
            }
            let pane_height = ui.available_height().max(500.0);
            let pane_inner_height = (pane_height - 12.0).max(480.0);

            ui.horizontal_top(|ui| {
                ui.spacing_mut().item_spacing.x = 0.0;
                ui.add_space(edge_guard);
                pane_frame(false).show(ui, |ui| {
                    let inner_width = (base_width - 14.0).max(0.0);
                    ui.vertical(|ui| {
                        ui.set_min_size(egui::vec2(inner_width, pane_inner_height));
                        ui.set_width(inner_width);
                        pane_header(ui, "Base Prices", Some(analysis.base_pricing.len()), None);
                        draw_pricing_table(
                            ui,
                            "base_pricing_table",
                            &analysis.base_pricing,
                            &analysis.base_currency,
                            &mut self.base_table,
                        );
                    });
                });
                ui.add_space(gap);
                pane_frame(false).show(ui, |ui| {
                    let inner_width = (selector_width - 14.0).max(0.0);
                    ui.vertical(|ui| {
                        ui.set_min_size(egui::vec2(inner_width, pane_inner_height));
                        ui.set_width(inner_width);
                        self.draw_iap_selector(ui, ctx, &analysis, inner_width);
                    });
                });
                ui.add_space(gap);
                pane_frame(self.loading_iap).show(ui, |ui| {
                    let inner_width = (detail_width - 14.0).max(0.0);
                    ui.vertical(|ui| {
                        ui.set_min_size(egui::vec2(inner_width, pane_inner_height));
                        ui.set_width(inner_width);
                        self.draw_iap_pricing_panel(ui, ctx, &analysis);
                    });
                });
                ui.add_space(edge_guard);
            });
        });
    }

    fn draw_metadata_cards(&self, ui: &mut egui::Ui, analysis: &AnalysisResult) {
        let gap = 6.0;
        let available = (ui.available_width() - gap * 3.0).max(0.0);
        let app_id_width = 220.0;
        let regions_width = 160.0;
        let iaps_width = 128.0;
        let app_name_width = (available - app_id_width - regions_width - iaps_width).max(260.0);
        ui.horizontal_top(|ui| {
            metadata_card(ui, "App Name", &analysis.display_name, app_name_width);
            ui.add_space(gap);
            metadata_card(ui, "App ID", &analysis.app_id, app_id_width);
            ui.add_space(gap);
            metadata_card(
                ui,
                "Regions",
                &analysis.base_pricing.len().to_string(),
                regions_width,
            );
            ui.add_space(gap);
            metadata_card(ui, "IAPs", &analysis.iaps.len().to_string(), iaps_width);
        });
    }

    fn draw_iap_selector(
        &mut self,
        ui: &mut egui::Ui,
        ctx: &egui::Context,
        analysis: &AnalysisResult,
        width: f32,
    ) {
        if analysis.iaps.is_empty() {
            pane_header(ui, "IAPs", Some(0), None);
            ui.label(RichText::new("No in-app purchase rows found.").color(mut_text_color()));
            return;
        }

        pane_header(ui, "IAPs", Some(analysis.iaps.len()), None);
        ui.add_space(4.0);
        ui.add_sized(
            [ui.available_width(), 24.0],
            egui::TextEdit::singleline(&mut self.iap_filter).hint_text("Filter purchases"),
        );
        ui.add_space(5.0);

        let filter = self.iap_filter.trim().to_lowercase();
        egui::ScrollArea::vertical()
            .id_salt("iap_rows_scroll")
            .auto_shrink([false, false])
            .max_height(ui.available_height().clamp(280.0, 760.0))
            .show(ui, |ui| {
                for (index, iap) in analysis.iaps.iter().enumerate() {
                    if !filter.is_empty()
                        && !iap.name.to_lowercase().contains(&filter)
                        && !iap.price_string.to_lowercase().contains(&filter)
                    {
                        continue;
                    }

                    let selected = self.selected_iap == Some(index);
                    let row_width = (ui.available_width().min(width) - 2.0).max(120.0);
                    if iap_row_button(ui, iap, selected, row_width).clicked() {
                        self.selected_iap = Some(index);
                        self.iap_result = None;
                        self.start_iap_pricing(ctx);
                    }
                    ui.add_space(3.0);
                }
            });
    }

    fn draw_iap_pricing_panel(
        &mut self,
        ui: &mut egui::Ui,
        ctx: &egui::Context,
        analysis: &AnalysisResult,
    ) {
        let selected = self
            .selected_iap
            .and_then(|index| analysis.iaps.get(index))
            .cloned();

        if selected_iap_header(ui, selected.as_ref(), self.loading_iap) {
            self.start_iap_pricing(ctx);
        }

        ui.add_space(5.0);
        if let Some(result) = &self.iap_result {
            draw_pricing_table(
                ui,
                "iap_pricing_table",
                &result.pricing,
                &analysis.base_currency,
                &mut self.iap_table,
            );
        } else {
            empty_panel(
                ui,
                "No IAP pricing loaded",
                "Select any IAP row to load regional pricing.",
            );
        }
    }

    fn draw_empty_state(&self, ui: &mut egui::Ui) {
        pane_frame(false).show(ui, |ui| {
            ui.set_min_height(ui.available_height().max(560.0));
            empty_panel(
                ui,
                "Enter an App Store link to begin",
                "Pricing tables appear here after the first analysis completes.",
            );
        });
    }
}

impl eframe::App for PricingGui {
    fn logic(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.receive_worker_messages(ctx);

        if self.loading_analysis || self.loading_iap {
            ctx.request_repaint_after(Duration::from_millis(80));
        }
    }

    fn ui(&mut self, ui: &mut egui::Ui, _frame: &mut eframe::Frame) {
        let ctx = ui.ctx().clone();
        ui.painter().rect_filled(ui.max_rect(), 0.0, bg_color());
        ui.set_min_size(ui.max_rect().size());
        egui::Frame::central_panel(ui.style())
            .fill(bg_color())
            .inner_margin(egui::Margin {
                left: 16,
                right: 16,
                top: 10,
                bottom: 12,
            })
            .show(ui, |ui| {
                ui.set_min_size(ui.available_size());
                let content_width = ui.available_width().min(APP_CONTENT_WIDTH);
                let left_space = ((ui.available_width() - content_width) / 2.0).max(0.0);
                ui.horizontal_top(|ui| {
                    ui.spacing_mut().item_spacing.x = 0.0;
                    ui.add_space(left_space);
                    ui.vertical(|ui| {
                        ui.set_width(content_width);
                        self.draw_input_panel(ui, &ctx);
                        ui.add_space(6.0);
                        self.draw_results(ui, &ctx);
                    });
                });
            });
    }
}

async fn analyze_app_pricing(
    client: reqwest::Client,
    link_or_id: String,
    base_currency: String,
) -> Result<AnalysisResult> {
    let app_id =
        extract_app_id(&link_or_id).context("Enter a valid App Store URL or numeric App ID.")?;
    let base_region = region_from_app_store_url(&link_or_id)
        .unwrap_or_else(|| default_region_for_currency(&base_currency));
    let display_name = fetch_app_name(&client, &app_id, base_region.code)
        .await
        .unwrap_or_else(|| app_id.clone());

    let regions = std::iter::once(base_region).chain(
        REGIONS
            .iter()
            .copied()
            .filter(|region| region.code != base_region.code),
    );
    let mut base_pricing: Vec<Pricing> = stream::iter(regions)
        .map(|region| {
            let client = client.clone();
            let app_id = app_id.clone();
            async move { collect_base_app_pricing(&client, &app_id, region).await }
        })
        .buffer_unordered(BASE_PRICE_CONCURRENCY)
        .filter_map(|pricing| async move { pricing })
        .collect()
        .await;

    if !base_pricing.is_empty() {
        let rates = get_conversion_rate(&client, &base_currency).await?;
        convert_prices(&mut base_pricing, &rates);
    }

    let iaps = fetch_iap_list_from_html(&client, &app_id, base_region.code)
        .await
        .unwrap_or_default();

    Ok(AnalysisResult {
        app_id,
        display_name,
        base_region,
        base_currency,
        base_pricing,
        iaps,
    })
}

async fn analyze_iap_pricing(
    client: reqwest::Client,
    analysis: AnalysisResult,
    selected_name: String,
    cached_region_iaps: HashMap<String, Vec<IAPItem>>,
    progress_tx: Sender<WorkerMessage>,
) -> Result<IapPricingResult> {
    let regions = iap_pricing_regions(&analysis);
    let total = regions.len();
    let rates = get_conversion_rate(&client, &analysis.base_currency).await?;
    let mut checked = 0;
    let mut pricing = Vec::new();
    let mut region_iap_cache = HashMap::new();
    let mut pending_pricing = Vec::new();
    let mut pending_cache = HashMap::new();
    let mut last_progress_checked = 0;
    let mut tasks = stream::iter(regions)
        .map(|region| {
            let client = client.clone();
            let app_id = analysis.app_id.clone();
            let selected_name = selected_name.clone();
            let cached_iaps = cached_region_iaps.get(region.code).cloned();
            async move {
                collect_iap_pricing_with_cache(
                    &client,
                    &app_id,
                    region,
                    &selected_name,
                    cached_iaps,
                )
                .await
            }
        })
        .buffer_unordered(IAP_PRICE_CONCURRENCY);

    while let Some((pricing_entry, cache_update)) = tasks.next().await {
        checked += 1;

        if let Some(mut pricing_entry) = pricing_entry {
            convert_price(&mut pricing_entry, &rates);
            pending_pricing.push(pricing_entry.clone());
            pricing.push(pricing_entry);
        }
        if let Some((region_code, iaps)) = cache_update {
            pending_cache.insert(region_code.clone(), iaps.clone());
            region_iap_cache.insert(region_code, iaps);
        }

        let should_report_progress =
            checked == total || checked - last_progress_checked >= IAP_PROGRESS_BATCH_SIZE;
        if should_report_progress {
            last_progress_checked = checked;
            let _ = progress_tx.send(WorkerMessage::IapPricingProgress {
                app_id: analysis.app_id.clone(),
                request_name: selected_name.clone(),
                checked,
                total,
                pricing: std::mem::take(&mut pending_pricing),
                region_iap_cache: std::mem::take(&mut pending_cache),
            });
        }
    }
    drop(tasks);

    sort_pricing_by_converted(&mut pricing);

    Ok(IapPricingResult {
        name: selected_name,
        pricing,
        region_iap_cache,
    })
}

async fn prefetch_iap_region_cache(
    client: reqwest::Client,
    analysis: AnalysisResult,
    cached_region_iaps: HashMap<String, Vec<IAPItem>>,
    tx: Sender<WorkerMessage>,
) {
    let app_id = analysis.app_id.clone();
    let regions = iap_pricing_regions(&analysis)
        .into_iter()
        .filter(|region| !cached_region_iaps.contains_key(region.code));
    let mut tasks = stream::iter(regions)
        .map(|region| {
            let client = client.clone();
            let app_id = app_id.clone();
            async move { fetch_iap_cache_entry(&client, &app_id, region).await }
        })
        .buffer_unordered(IAP_PREFETCH_CONCURRENCY);
    let mut pending_cache = HashMap::new();

    while let Some(cache_entry) = tasks.next().await {
        if let Some((region_code, iaps)) = cache_entry {
            pending_cache.insert(region_code, iaps);
        }

        if pending_cache.len() >= IAP_PROGRESS_BATCH_SIZE {
            let _ = tx.send(WorkerMessage::IapCacheBatch {
                app_id: app_id.clone(),
                region_iap_cache: std::mem::take(&mut pending_cache),
            });
        }
    }
    drop(tasks);

    if !pending_cache.is_empty() {
        let _ = tx.send(WorkerMessage::IapCacheBatch {
            app_id,
            region_iap_cache: pending_cache,
        });
    }
}

async fn fetch_iap_cache_entry(
    client: &reqwest::Client,
    app_id: &str,
    region: Region,
) -> Option<(String, Vec<IAPItem>)> {
    fetch_iap_list_from_html(client, app_id, region.code)
        .await
        .ok()
        .map(|iaps| (region.code.to_string(), iaps))
}

fn iap_pricing_regions(analysis: &AnalysisResult) -> Vec<Region> {
    if analysis.base_pricing.is_empty() {
        return REGIONS.to_vec();
    }

    let available_codes: HashSet<&str> = analysis
        .base_pricing
        .iter()
        .map(|pricing| pricing.region_code.as_str())
        .collect();

    REGIONS
        .iter()
        .copied()
        .filter(|region| available_codes.contains(region.code))
        .collect()
}

fn install_system_currency_font(ctx: &egui::Context) {
    let Some(font_bytes) = system_currency_font_bytes() else {
        return;
    };

    let font_name = "system_currency_symbols".to_string();
    let mut fonts = egui::FontDefinitions::default();
    fonts.font_data.insert(
        font_name.clone(),
        std::sync::Arc::new(egui::FontData::from_owned(font_bytes)),
    );

    for family in [egui::FontFamily::Proportional, egui::FontFamily::Monospace] {
        if let Some(family_fonts) = fonts.families.get_mut(&family) {
            family_fonts.push(font_name.clone());
        }
    }

    ctx.set_fonts(fonts);
}

fn system_currency_font_bytes() -> Option<Vec<u8>> {
    system_currency_font_paths()
        .iter()
        .find_map(|path| std::fs::read(path).ok())
}

fn system_currency_font_paths() -> &'static [&'static str] {
    &[
        "/System/Library/Fonts/SFNS.ttf",
        "/System/Library/Fonts/SFCompact.ttf",
        "/System/Library/Fonts/Supplemental/Arial.ttf",
        "/System/Library/Fonts/Supplemental/Arial Unicode.ttf",
        "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        "C:\\Windows\\Fonts\\arial.ttf",
    ]
}

fn apply_gui_style(ctx: &egui::Context) {
    let mut style = (*ctx.global_style()).clone();
    style.visuals = egui::Visuals::light();
    style.visuals.override_text_color = Some(text_color());
    style.visuals.weak_text_color = Some(mut_text_color());
    style.visuals.panel_fill = bg_color();
    style.visuals.window_fill = surface_color();
    style.visuals.window_stroke = egui::Stroke::new(1.0, border_color());
    style.visuals.window_corner_radius = egui::CornerRadius::same(FRAME_RADIUS);
    style.visuals.extreme_bg_color = control_bg_color();
    style.visuals.text_edit_bg_color = Some(control_bg_color());
    style.visuals.code_bg_color = header_bg_color();
    style.visuals.faint_bg_color = stripe_color();
    style.visuals.widgets.noninteractive.bg_fill = surface_color();
    style.visuals.widgets.noninteractive.fg_stroke = egui::Stroke::new(1.0, text_color());
    style.visuals.widgets.inactive.bg_fill = control_bg_color();
    style.visuals.widgets.inactive.weak_bg_fill = control_bg_color();
    style.visuals.widgets.inactive.bg_stroke = egui::Stroke::new(1.0, border_color());
    style.visuals.widgets.inactive.fg_stroke = egui::Stroke::new(1.0, text_color());
    style.visuals.widgets.hovered.bg_fill = row_hover_color();
    style.visuals.widgets.hovered.weak_bg_fill = row_hover_color();
    style.visuals.widgets.hovered.bg_stroke = egui::Stroke::new(1.0, selection_border_color());
    style.visuals.widgets.hovered.fg_stroke = egui::Stroke::new(1.0, text_color());
    style.visuals.widgets.active.bg_fill = accent_soft_color();
    style.visuals.widgets.active.weak_bg_fill = accent_soft_color();
    style.visuals.widgets.active.bg_stroke = egui::Stroke::new(1.0, selection_border_color());
    style.visuals.widgets.active.fg_stroke = egui::Stroke::new(1.0, text_color());
    style.visuals.selection.bg_fill = accent_color();
    style.visuals.striped = true;
    style.visuals.interact_cursor = Some(egui::CursorIcon::PointingHand);
    style.visuals.widgets.noninteractive.corner_radius = egui::CornerRadius::same(FRAME_RADIUS);
    style.visuals.widgets.inactive.corner_radius = egui::CornerRadius::same(CONTROL_RADIUS);
    style.visuals.widgets.hovered.corner_radius = egui::CornerRadius::same(CONTROL_RADIUS);
    style.visuals.widgets.active.corner_radius = egui::CornerRadius::same(CONTROL_RADIUS);
    style.visuals.widgets.open.corner_radius = egui::CornerRadius::same(CONTROL_RADIUS);
    style.spacing.item_spacing = egui::vec2(4.0, 3.0);
    style.spacing.button_padding = egui::vec2(7.0, 4.0);
    ctx.set_global_style(style);
}

fn bg_color() -> Color32 {
    Color32::from_rgb(242, 246, 251)
}

fn surface_color() -> Color32 {
    Color32::from_rgb(253, 254, 255)
}

fn pane_color() -> Color32 {
    Color32::from_rgb(248, 251, 255)
}

fn selected_pane_color() -> Color32 {
    Color32::from_rgb(246, 250, 255)
}

fn workspace_color() -> Color32 {
    Color32::from_rgb(237, 243, 249)
}

fn control_bg_color() -> Color32 {
    Color32::from_rgb(249, 252, 255)
}

fn header_bg_color() -> Color32 {
    Color32::from_rgb(234, 240, 248)
}

fn stripe_color() -> Color32 {
    Color32::from_rgb(247, 250, 254)
}

fn row_hover_color() -> Color32 {
    Color32::from_rgb(238, 246, 255)
}

fn accent_soft_color() -> Color32 {
    Color32::from_rgb(228, 241, 255)
}

fn border_color() -> Color32 {
    Color32::from_rgb(190, 202, 217)
}

fn hairline_color() -> Color32 {
    Color32::from_rgb(221, 230, 241)
}

fn text_color() -> Color32 {
    Color32::from_rgb(20, 24, 31)
}

fn mut_text_color() -> Color32 {
    Color32::from_rgb(82, 91, 105)
}

fn accent_color() -> Color32 {
    Color32::from_rgb(0, 102, 204)
}

fn selection_border_color() -> Color32 {
    Color32::from_rgb(82, 143, 226)
}

fn warn_text_color() -> Color32 {
    Color32::from_rgb(134, 82, 0)
}

fn toolbar_frame() -> egui::Frame {
    egui::Frame::new()
        .fill(surface_color())
        .corner_radius(FRAME_RADIUS)
        .stroke(egui::Stroke::new(1.0, border_color()))
        .inner_margin(egui::Margin::symmetric(8, 6))
}

fn main_workspace_frame() -> egui::Frame {
    egui::Frame::new()
        .fill(workspace_color())
        .corner_radius(FRAME_RADIUS)
        .stroke(egui::Stroke::new(1.0, border_color()))
        .inner_margin(egui::Margin::same(6))
}

fn pane_frame(selected: bool) -> egui::Frame {
    let stroke = if selected {
        egui::Stroke::new(1.0, accent_color())
    } else {
        egui::Stroke::new(1.0, border_color())
    };
    let fill = if selected {
        selected_pane_color()
    } else {
        pane_color()
    };

    egui::Frame::new()
        .fill(fill)
        .corner_radius(FRAME_RADIUS)
        .stroke(stroke)
        .inner_margin(egui::Margin::same(7))
}

fn notice_frame() -> egui::Frame {
    egui::Frame::new()
        .fill(Color32::from_rgb(255, 249, 237))
        .corner_radius(FRAME_RADIUS)
        .stroke(egui::Stroke::new(1.0, Color32::from_rgb(240, 211, 158)))
        .inner_margin(egui::Margin::same(6))
}

fn metadata_card(ui: &mut egui::Ui, label: &str, value: &str, width: f32) {
    egui::Frame::new()
        .fill(control_bg_color())
        .corner_radius(FRAME_RADIUS)
        .stroke(egui::Stroke::new(1.0, border_color()))
        .inner_margin(egui::Margin::symmetric(8, 5))
        .show(ui, |ui| {
            ui.horizontal(|ui| {
                let inner_width = (width - 16.0).max(72.0);
                ui.set_min_width(inner_width);
                ui.set_width(inner_width);
                let label_width = inner_width.min(70.0);
                ui.add_sized(
                    [label_width, 20.0],
                    egui::Label::new(
                        RichText::new(label.to_ascii_uppercase())
                            .size(9.5)
                            .strong()
                            .color(mut_text_color()),
                    )
                    .truncate(),
                );
                let value_width = (inner_width - label_width - 6.0).max(48.0);
                ui.add_sized(
                    [value_width, 20.0],
                    egui::Label::new(RichText::new(value).size(13.0).strong().color(text_color()))
                        .truncate(),
                )
                .on_hover_text(value);
            });
        });
}

fn pane_header(ui: &mut egui::Ui, title: &str, count: Option<usize>, subtitle: Option<&str>) {
    ui.horizontal(|ui| {
        ui.vertical(|ui| {
            ui.horizontal(|ui| {
                ui.add_sized(
                    [(ui.available_width() - 34.0).max(80.0), 21.0],
                    egui::Label::new(RichText::new(title).size(13.0).strong().color(text_color()))
                        .truncate(),
                );
                if let Some(count) = count {
                    ui.add_sized(
                        [30.0, 21.0],
                        egui::Label::new(
                            RichText::new(count.to_string())
                                .size(12.0)
                                .strong()
                                .color(mut_text_color()),
                        )
                        .truncate(),
                    );
                }
            });
            if let Some(subtitle) = subtitle {
                ui.add_sized(
                    [ui.available_width(), 20.0],
                    egui::Label::new(
                        RichText::new(subtitle)
                            .size(12.0)
                            .strong()
                            .color(accent_color()),
                    )
                    .truncate(),
                )
                .on_hover_text(subtitle);
            }
        });
    });
}

fn compact_field_label(text: &str) -> RichText {
    RichText::new(text)
        .size(12.0)
        .strong()
        .color(mut_text_color())
}

fn primary_button(label: &str) -> egui::Button<'_> {
    egui::Button::new(RichText::new(label).color(Color32::WHITE).strong())
        .fill(accent_color())
        .corner_radius(CONTROL_RADIUS)
}

fn empty_panel(ui: &mut egui::Ui, title: &str, body: &str) {
    ui.allocate_ui_with_layout(
        [ui.available_width(), 110.0].into(),
        egui::Layout::top_down(egui::Align::Center),
        |ui| {
            ui.add_space(28.0);
            ui.label(RichText::new(title).size(13.0).strong().color(text_color()));
            ui.add_space(2.0);
            ui.label(RichText::new(body).size(11.0).color(mut_text_color()));
        },
    );
}

fn iap_row_button(ui: &mut egui::Ui, iap: &IAPItem, selected: bool, width: f32) -> egui::Response {
    let row_width = width.max(120.0);
    let (rect, response) =
        ui.allocate_exact_size(egui::vec2(row_width, 26.0), egui::Sense::click());

    let fill = if selected {
        accent_soft_color()
    } else if response.hovered() {
        row_hover_color()
    } else {
        surface_color()
    };
    let stroke = if selected || response.hovered() {
        egui::Stroke::new(1.0, selection_border_color())
    } else {
        egui::Stroke::new(1.0, hairline_color())
    };

    ui.painter().rect_filled(rect, ROW_RADIUS, fill);
    if selected {
        let rail_rect =
            egui::Rect::from_min_max(rect.min, egui::pos2(rect.left() + 2.0, rect.bottom()));
        ui.painter().rect_filled(rail_rect, 0.0, accent_color());
    }
    ui.painter()
        .rect_stroke(rect, ROW_RADIUS, stroke, egui::StrokeKind::Inside);

    let inner = rect.shrink2(egui::vec2(7.0, 3.0));
    let price_width = 58.0;
    let gap = 5.0;
    let price_rect = egui::Rect::from_min_max(
        egui::pos2(inner.right() - price_width, inner.top()),
        egui::pos2(inner.right(), inner.bottom()),
    );
    let name_rect = egui::Rect::from_min_max(
        inner.min,
        egui::pos2((price_rect.left() - gap).max(inner.left()), inner.bottom()),
    );
    let display_name = display_iap_name(&iap.name);

    paint_clipped_text(
        ui,
        name_rect,
        &display_name,
        egui::FontId::proportional(11.5),
        if selected {
            accent_color()
        } else {
            text_color()
        },
        egui::Align2::LEFT_CENTER,
    );
    paint_clipped_text(
        ui,
        price_rect,
        &iap.price_string,
        egui::FontId::monospace(11.0),
        mut_text_color(),
        egui::Align2::RIGHT_CENTER,
    );

    if response.hovered() {
        ui.ctx().set_cursor_icon(egui::CursorIcon::PointingHand);
    }

    response.on_hover_text(format!("{}  {}", iap.name, iap.price_string))
}

fn selected_iap_header(ui: &mut egui::Ui, selected: Option<&IAPItem>, loading: bool) -> bool {
    let width = ui.available_width().max(180.0);
    let (rect, row_response) =
        ui.allocate_exact_size(egui::vec2(width, 28.0), egui::Sense::hover());
    ui.painter()
        .rect_filled(rect, ROW_RADIUS, control_bg_color());
    ui.painter().rect_stroke(
        rect,
        ROW_RADIUS,
        egui::Stroke::new(1.0, hairline_color()),
        egui::StrokeKind::Inside,
    );

    let inner = rect.shrink2(egui::vec2(7.0, 3.0));
    let label_width = 76.0;
    let price_width = 70.0;
    let button_width = 52.0;
    let gap = 10.0;
    let button_rect = egui::Rect::from_min_size(
        egui::pos2(inner.right() - button_width, inner.top()),
        egui::vec2(button_width, inner.height()),
    );
    let price_rect = egui::Rect::from_min_size(
        egui::pos2(button_rect.left() - gap - price_width, inner.top()),
        egui::vec2(price_width, inner.height()),
    );
    let label_rect = egui::Rect::from_min_size(inner.min, egui::vec2(label_width, inner.height()));
    let name_rect = egui::Rect::from_min_max(
        egui::pos2(label_rect.right() + gap, inner.top()),
        egui::pos2(
            (price_rect.left() - gap).max(label_rect.right() + gap),
            inner.bottom(),
        ),
    );

    let (name, price, hover_text) = selected
        .map(|iap| {
            (
                display_iap_name(&iap.name),
                iap.price_string.as_str(),
                Some(iap.name.as_str()),
            )
        })
        .unwrap_or_else(|| ("No IAP selected".to_string(), "", None));

    paint_clipped_text(
        ui,
        label_rect,
        "Selected",
        egui::FontId::proportional(12.0),
        mut_text_color(),
        egui::Align2::LEFT_CENTER,
    );
    paint_clipped_text(
        ui,
        name_rect,
        &name,
        egui::FontId::proportional(12.0),
        accent_color(),
        egui::Align2::LEFT_CENTER,
    );
    paint_clipped_text(
        ui,
        price_rect,
        price,
        egui::FontId::monospace(12.0),
        text_color(),
        egui::Align2::RIGHT_CENTER,
    );

    if let Some(hover_text) = hover_text {
        row_response.on_hover_text(hover_text);
    }

    let enabled = selected.is_some() && !loading;
    let response = ui.interact(
        button_rect,
        ui.id().with("selected_iap_check_button"),
        egui::Sense::click(),
    );
    let fill = if enabled {
        if response.hovered() {
            Color32::from_rgb(23, 126, 238)
        } else {
            accent_color()
        }
    } else {
        Color32::from_rgb(142, 180, 232)
    };
    ui.painter().rect_filled(button_rect, ROW_RADIUS, fill);
    paint_clipped_text(
        ui,
        button_rect,
        if loading { "..." } else { "Check" },
        egui::FontId::proportional(12.0),
        Color32::WHITE,
        egui::Align2::CENTER_CENTER,
    );

    enabled && response.clicked()
}

fn paint_clipped_text(
    ui: &egui::Ui,
    rect: egui::Rect,
    text: &str,
    font: egui::FontId,
    color: Color32,
    align: egui::Align2,
) {
    let painter = ui.painter().with_clip_rect(rect);
    let pos = match align {
        egui::Align2::LEFT_CENTER => egui::pos2(rect.left() + 2.0, rect.center().y),
        egui::Align2::RIGHT_CENTER => egui::pos2(rect.right() - 2.0, rect.center().y),
        egui::Align2::CENTER_CENTER => rect.center(),
        _ => rect.center(),
    };
    painter.text(pos, align, text, font, color);
}

fn draw_pricing_table(
    ui: &mut egui::Ui,
    id: &str,
    pricing: &[Pricing],
    base_currency: &str,
    state: &mut PricingTableState,
) {
    if pricing.is_empty() {
        ui.label(RichText::new("No pricing data found.").color(mut_text_color()));
        return;
    }

    ui.horizontal(|ui| {
        ui.add_sized(
            [(ui.available_width() - 62.0).max(142.0), 24.0],
            egui::TextEdit::singleline(&mut state.filter).hint_text("Filter regions or currency"),
        );
        let visible = pricing
            .iter()
            .filter(|entry| pricing_matches_filter(entry, &state.filter))
            .count();
        ui.with_layout(egui::Layout::right_to_left(egui::Align::Center), |ui| {
            ui.label(
                RichText::new(format!("{visible}/{}", pricing.len()))
                    .size(11.0)
                    .color(mut_text_color()),
            );
        });
    });
    ui.add_space(4.0);

    let mut rows: Vec<&Pricing> = pricing
        .iter()
        .filter(|entry| pricing_matches_filter(entry, &state.filter))
        .collect();
    sort_pricing_rows(&mut rows, state);

    let table_height = ui.available_height().clamp(220.0, 760.0);
    TableBuilder::new(ui)
        .id_salt(id)
        .striped(true)
        .resizable(false)
        .auto_shrink([false, false])
        .max_scroll_height(table_height)
        .min_scrolled_height(160.0)
        .cell_layout(egui::Layout::left_to_right(egui::Align::Center))
        .column(Column::remainder().at_least(74.0).clip(true))
        .column(
            Column::initial(74.0)
                .at_least(58.0)
                .at_most(112.0)
                .clip(true),
        )
        .column(
            Column::initial(42.0)
                .at_least(36.0)
                .at_most(52.0)
                .clip(true),
        )
        .column(
            Column::initial(78.0)
                .at_least(62.0)
                .at_most(122.0)
                .clip(true),
        )
        .header(TABLE_HEADER_HEIGHT, |mut header| {
            header.col(|ui| {
                table_header_cell(ui, "Region", PricingSort::Region, state, false);
            });
            header.col(|ui| {
                table_header_cell(ui, "Price", PricingSort::Price, state, true);
            });
            header.col(|ui| {
                table_header_cell(ui, "Cur", PricingSort::Currency, state, true);
            });
            header.col(|ui| {
                table_header_cell(ui, base_currency, PricingSort::Converted, state, true);
            });
        })
        .body(|body| {
            body.rows(TABLE_ROW_HEIGHT, rows.len(), |mut row| {
                let entry = rows[row.index()];
                row.col(|ui| {
                    table_text_cell(ui, &entry.region, text_color(), false, false, false)
                        .on_hover_text(&entry.region);
                });
                row.col(|ui| {
                    table_text_cell(
                        ui,
                        &format_price(entry.amount, &entry.currency),
                        text_color(),
                        true,
                        true,
                        true,
                    );
                });
                row.col(|ui| {
                    table_text_cell(ui, &entry.currency, mut_text_color(), false, false, true);
                });
                row.col(|ui| {
                    table_text_cell(
                        ui,
                        &entry
                            .converted_amount
                            .map(|amount| format_price(amount, base_currency))
                            .unwrap_or_else(|| "N/A".to_string()),
                        text_color(),
                        true,
                        true,
                        true,
                    );
                });
            });
        });
}

const TABLE_HEADER_HEIGHT: f32 = 24.0;
const TABLE_ROW_HEIGHT: f32 = 22.0;

fn table_header_cell(
    ui: &mut egui::Ui,
    label: &str,
    sort: PricingSort,
    state: &mut PricingTableState,
    right_align: bool,
) {
    let marker = if state.sort == sort {
        if state.ascending {
            " ^"
        } else {
            " v"
        }
    } else {
        ""
    };
    let text = format!("{label}{marker}");
    let response = paint_table_cell(
        ui,
        &text,
        text_color(),
        true,
        false,
        right_align,
        egui::Sense::click(),
    );
    if response.clicked() {
        state.toggle_sort(sort);
    }
}

fn table_text_cell(
    ui: &mut egui::Ui,
    text: &str,
    color: Color32,
    strong: bool,
    monospace: bool,
    right_align: bool,
) -> egui::Response {
    paint_table_cell(
        ui,
        text,
        color,
        strong,
        monospace,
        right_align,
        egui::Sense::hover(),
    )
}

fn paint_table_cell(
    ui: &mut egui::Ui,
    text: &str,
    color: Color32,
    strong: bool,
    monospace: bool,
    right_align: bool,
    sense: egui::Sense,
) -> egui::Response {
    let rect_height = ui.available_height().max(TABLE_ROW_HEIGHT);
    let (rect, response) =
        ui.allocate_exact_size(egui::vec2(ui.available_width(), rect_height), sense);
    if sense == egui::Sense::click() {
        ui.painter().rect_filled(
            rect.shrink2(egui::vec2(0.0, 1.0)),
            TABLE_HEADER_RADIUS,
            header_bg_color(),
        );
    }
    if response.hovered() && sense == egui::Sense::click() {
        ui.painter().rect_filled(
            rect.shrink2(egui::vec2(0.0, 1.0)),
            TABLE_HEADER_RADIUS,
            row_hover_color(),
        );
    }
    ui.painter().line_segment(
        [
            egui::pos2(rect.left(), rect.bottom() - 0.5),
            egui::pos2(rect.right(), rect.bottom() - 0.5),
        ],
        egui::Stroke::new(1.0, hairline_color()),
    );

    let font = if monospace {
        egui::FontId::monospace(11.0)
    } else {
        egui::FontId::proportional(if strong { 11.2 } else { 11.0 })
    };
    let align = if right_align {
        egui::Align2::RIGHT_CENTER
    } else {
        egui::Align2::LEFT_CENTER
    };
    let pos = if right_align {
        egui::pos2(rect.right() - 3.0, rect.center().y)
    } else {
        egui::pos2(rect.left() + 3.0, rect.center().y)
    };
    ui.painter().text(pos, align, text, font, color);
    response
}

fn pricing_matches_filter(entry: &Pricing, filter: &str) -> bool {
    let filter = filter.trim().to_lowercase();
    filter.is_empty()
        || entry.region.to_lowercase().contains(&filter)
        || entry.currency.to_lowercase().contains(&filter)
}

fn sort_pricing_rows(rows: &mut [&Pricing], state: &PricingTableState) {
    rows.sort_by(|a, b| compare_pricing(a, b, state.sort));
    if !state.ascending {
        rows.reverse();
    }
}

fn compare_pricing(a: &Pricing, b: &Pricing, sort: PricingSort) -> Ordering {
    let ordering = match sort {
        PricingSort::Region => a.region.cmp(&b.region),
        PricingSort::Price => a.amount.total_cmp(&b.amount),
        PricingSort::Currency => a.currency.cmp(&b.currency),
        PricingSort::Converted => compare_optional_amount(a.converted_amount, b.converted_amount),
    };

    ordering.then_with(|| a.region.cmp(&b.region))
}

fn compare_optional_amount(a: Option<f64>, b: Option<f64>) -> Ordering {
    match (a, b) {
        (Some(a), Some(b)) => a.total_cmp(&b),
        (Some(_), None) => Ordering::Less,
        (None, Some(_)) => Ordering::Greater,
        (None, None) => Ordering::Equal,
    }
}

#[derive(Copy, Clone)]
struct IconColor {
    r: u8,
    g: u8,
    b: u8,
    a: u8,
}

fn app_icon() -> egui::IconData {
    let size = APP_ICON_SIZE;
    let mut rgba = vec![0; (size * size * 4) as usize];

    for y in 0..size {
        for x in 0..size {
            let fx = x as f32 + 0.5;
            let fy = y as f32 + 0.5;
            let coverage = rounded_rect_coverage(fx, fy, 6.0, 6.0, 122.0, 122.0, 28.0);
            if coverage <= 0.0 {
                continue;
            }

            let horizontal = fx / size as f32;
            let vertical = 1.0 - fy / size as f32;
            let color = mix_icon_color(
                IconColor {
                    r: 6,
                    g: 20,
                    b: 44,
                    a: 255,
                },
                IconColor {
                    r: 0,
                    g: 102,
                    b: 204,
                    a: 255,
                },
                (horizontal * 0.35 + vertical * 0.65).clamp(0.0, 1.0),
            );
            blend_icon_pixel(&mut rgba, size, x, y, with_alpha(color, coverage));
        }
    }

    paint_rounded_icon_rect(
        &mut rgba,
        size,
        IconRect::new(22.0, 24.0, 106.0, 104.0, 15.0),
        IconColor {
            r: 248,
            g: 252,
            b: 255,
            a: 238,
        },
    );
    paint_rounded_icon_rect(
        &mut rgba,
        size,
        IconRect::new(32.0, 38.0, 96.0, 49.0, 4.0),
        IconColor {
            r: 0,
            g: 102,
            b: 204,
            a: 230,
        },
    );

    for (y, width) in [(60.0, 32.0), (74.0, 25.0), (88.0, 36.0)] {
        paint_rounded_icon_rect(
            &mut rgba,
            size,
            IconRect::new(34.0, y, 34.0 + width, y + 5.0, 2.0),
            IconColor {
                r: 103,
                g: 117,
                b: 137,
                a: 160,
            },
        );
    }

    for (x, y, width, color) in [
        (
            76.0,
            59.0,
            19.0,
            IconColor {
                r: 19,
                g: 188,
                b: 235,
                a: 230,
            },
        ),
        (
            74.0,
            73.0,
            21.0,
            IconColor {
                r: 0,
                g: 102,
                b: 204,
                a: 225,
            },
        ),
        (
            72.0,
            87.0,
            23.0,
            IconColor {
                r: 33,
                g: 212,
                b: 177,
                a: 230,
            },
        ),
    ] {
        paint_rounded_icon_rect(
            &mut rgba,
            size,
            IconRect::new(x, y, x + width, y + 6.0, 3.0),
            color,
        );
    }

    paint_icon_line(
        &mut rgba,
        size,
        IconPoint { x: 33.0, y: 95.0 },
        IconPoint { x: 52.0, y: 80.0 },
        4.0,
        IconColor {
            r: 0,
            g: 102,
            b: 204,
            a: 225,
        },
    );
    paint_icon_line(
        &mut rgba,
        size,
        IconPoint { x: 52.0, y: 80.0 },
        IconPoint { x: 72.0, y: 86.0 },
        4.0,
        IconColor {
            r: 0,
            g: 102,
            b: 204,
            a: 225,
        },
    );
    paint_icon_line(
        &mut rgba,
        size,
        IconPoint { x: 72.0, y: 86.0 },
        IconPoint { x: 96.0, y: 58.0 },
        4.0,
        IconColor {
            r: 0,
            g: 102,
            b: 204,
            a: 225,
        },
    );
    for point in [
        IconPoint { x: 33.0, y: 95.0 },
        IconPoint { x: 52.0, y: 80.0 },
        IconPoint { x: 72.0, y: 86.0 },
        IconPoint { x: 96.0, y: 58.0 },
    ] {
        paint_icon_circle(
            &mut rgba,
            size,
            point,
            4.8,
            IconColor {
                r: 247,
                g: 251,
                b: 255,
                a: 255,
            },
        );
        paint_icon_circle(
            &mut rgba,
            size,
            point,
            2.7,
            IconColor {
                r: 0,
                g: 102,
                b: 204,
                a: 255,
            },
        );
    }

    egui::IconData {
        rgba,
        width: size,
        height: size,
    }
}

#[derive(Copy, Clone)]
struct IconRect {
    left: f32,
    top: f32,
    right: f32,
    bottom: f32,
    radius: f32,
}

impl IconRect {
    fn new(left: f32, top: f32, right: f32, bottom: f32, radius: f32) -> Self {
        Self {
            left,
            top,
            right,
            bottom,
            radius,
        }
    }
}

#[derive(Copy, Clone)]
struct IconPoint {
    x: f32,
    y: f32,
}

fn paint_rounded_icon_rect(rgba: &mut [u8], size: u32, rect: IconRect, color: IconColor) {
    let min_x = rect.left.floor().max(0.0) as u32;
    let min_y = rect.top.floor().max(0.0) as u32;
    let max_x = rect.right.ceil().min(size as f32) as u32;
    let max_y = rect.bottom.ceil().min(size as f32) as u32;

    for y in min_y..max_y {
        for x in min_x..max_x {
            let coverage = rounded_rect_coverage(
                x as f32 + 0.5,
                y as f32 + 0.5,
                rect.left,
                rect.top,
                rect.right,
                rect.bottom,
                rect.radius,
            );
            if coverage > 0.0 {
                blend_icon_pixel(rgba, size, x, y, with_alpha(color, coverage));
            }
        }
    }
}

fn paint_icon_circle(rgba: &mut [u8], size: u32, center: IconPoint, radius: f32, color: IconColor) {
    let min_x = (center.x - radius - 1.0).floor().max(0.0) as u32;
    let min_y = (center.y - radius - 1.0).floor().max(0.0) as u32;
    let max_x = (center.x + radius + 1.0).ceil().min(size as f32) as u32;
    let max_y = (center.y + radius + 1.0).ceil().min(size as f32) as u32;

    for y in min_y..max_y {
        for x in min_x..max_x {
            let dx = x as f32 + 0.5 - center.x;
            let dy = y as f32 + 0.5 - center.y;
            let distance = (dx * dx + dy * dy).sqrt() - radius;
            let coverage = (0.5 - distance).clamp(0.0, 1.0);
            if coverage > 0.0 {
                blend_icon_pixel(rgba, size, x, y, with_alpha(color, coverage));
            }
        }
    }
}

fn paint_icon_line(
    rgba: &mut [u8],
    size: u32,
    start: IconPoint,
    end: IconPoint,
    width: f32,
    color: IconColor,
) {
    let half_width = width / 2.0;
    let min_x = (start.x.min(end.x) - half_width - 1.0).floor().max(0.0) as u32;
    let min_y = (start.y.min(end.y) - half_width - 1.0).floor().max(0.0) as u32;
    let max_x = (start.x.max(end.x) + half_width + 1.0)
        .ceil()
        .min(size as f32) as u32;
    let max_y = (start.y.max(end.y) + half_width + 1.0)
        .ceil()
        .min(size as f32) as u32;

    for y in min_y..max_y {
        for x in min_x..max_x {
            let point = IconPoint {
                x: x as f32 + 0.5,
                y: y as f32 + 0.5,
            };
            let distance = distance_to_segment(point, start, end) - half_width;
            let coverage = (0.5 - distance).clamp(0.0, 1.0);
            if coverage > 0.0 {
                blend_icon_pixel(rgba, size, x, y, with_alpha(color, coverage));
            }
        }
    }
}

fn rounded_rect_coverage(
    x: f32,
    y: f32,
    left: f32,
    top: f32,
    right: f32,
    bottom: f32,
    radius: f32,
) -> f32 {
    let half_width = (right - left) / 2.0 - radius;
    let half_height = (bottom - top) / 2.0 - radius;
    let center_x = (left + right) / 2.0;
    let center_y = (top + bottom) / 2.0;
    let dx = (x - center_x).abs() - half_width;
    let dy = (y - center_y).abs() - half_height;
    let outside_x = dx.max(0.0);
    let outside_y = dy.max(0.0);
    let distance =
        (outside_x * outside_x + outside_y * outside_y).sqrt() + dx.max(dy).min(0.0) - radius;
    (0.5 - distance).clamp(0.0, 1.0)
}

fn distance_to_segment(point: IconPoint, start: IconPoint, end: IconPoint) -> f32 {
    let segment_x = end.x - start.x;
    let segment_y = end.y - start.y;
    let length_squared = segment_x * segment_x + segment_y * segment_y;
    if length_squared == 0.0 {
        let dx = point.x - start.x;
        let dy = point.y - start.y;
        return (dx * dx + dy * dy).sqrt();
    }

    let projection = (((point.x - start.x) * segment_x + (point.y - start.y) * segment_y)
        / length_squared)
        .clamp(0.0, 1.0);
    let closest_x = start.x + projection * segment_x;
    let closest_y = start.y + projection * segment_y;
    let dx = point.x - closest_x;
    let dy = point.y - closest_y;
    (dx * dx + dy * dy).sqrt()
}

fn mix_icon_color(start: IconColor, end: IconColor, amount: f32) -> IconColor {
    let mix = |a: u8, b: u8| (a as f32 + (b as f32 - a as f32) * amount).round() as u8;
    IconColor {
        r: mix(start.r, end.r),
        g: mix(start.g, end.g),
        b: mix(start.b, end.b),
        a: mix(start.a, end.a),
    }
}

fn with_alpha(color: IconColor, coverage: f32) -> IconColor {
    IconColor {
        a: (color.a as f32 * coverage).round().clamp(0.0, 255.0) as u8,
        ..color
    }
}

fn blend_icon_pixel(rgba: &mut [u8], size: u32, x: u32, y: u32, source: IconColor) {
    let index = ((y * size + x) * 4) as usize;
    let source_alpha = source.a as f32 / 255.0;
    let target_alpha = rgba[index + 3] as f32 / 255.0;
    let out_alpha = source_alpha + target_alpha * (1.0 - source_alpha);
    if out_alpha <= f32::EPSILON {
        return;
    }

    let blend = |source_channel: u8, target_channel: u8| {
        ((source_channel as f32 * source_alpha
            + target_channel as f32 * target_alpha * (1.0 - source_alpha))
            / out_alpha)
            .round()
            .clamp(0.0, 255.0) as u8
    };

    rgba[index] = blend(source.r, rgba[index]);
    rgba[index + 1] = blend(source.g, rgba[index + 1]);
    rgba[index + 2] = blend(source.b, rgba[index + 2]);
    rgba[index + 3] = (out_alpha * 255.0).round().clamp(0.0, 255.0) as u8;
}

fn main() -> eframe::Result<()> {
    let native_options = eframe::NativeOptions {
        viewport: egui::ViewportBuilder::default()
            .with_inner_size([APP_WINDOW_WIDTH, 820.0])
            .with_min_inner_size([1120.0, 680.0])
            .with_icon(app_icon()),
        centered: true,
        persist_window: false,
        ..Default::default()
    };

    eframe::run_native(
        "App Store Pricing",
        native_options,
        Box::new(|cc| Ok(Box::new(PricingGui::new(cc)))),
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_price(input: &str, expected: f64) {
        let actual =
            parse_price_from_string_with_currency(input, None).expect("price should parse");
        assert!(
            (actual - expected).abs() < 0.001,
            "expected {expected}, got {actual}"
        );
    }

    fn assert_price_for_currency(input: &str, currency: &str, expected: f64) {
        let actual =
            parse_price_from_string_for_currency(input, currency).expect("price should parse");
        assert!(
            (actual - expected).abs() < 0.001,
            "expected {expected}, got {actual}"
        );
    }

    #[test]
    fn extracts_app_id_from_plain_id_and_urls() {
        assert_eq!(extract_app_id(""), None);
        assert_eq!(extract_app_id("123456789"), Some("123456789".to_string()));
        assert_eq!(extract_app_id("id123456789"), Some("123456789".to_string()));
        assert_eq!(
            extract_app_id("https://apps.apple.com/us/app/example/id123456789?uo=4"),
            Some("123456789".to_string())
        );
    }

    #[test]
    fn derives_reasonable_base_region() {
        assert_eq!(
            region_from_app_store_url("https://apps.apple.com/sg/app/example/id123")
                .expect("region should parse")
                .code,
            "SG"
        );
        assert_eq!(default_region_for_currency("EUR").code, "DE");
        assert_eq!(default_region_for_currency("SGD").code, "SG");
    }

    #[test]
    fn distinguishes_storefront_currency_from_country_currency() {
        assert_eq!(currency_for_region("DZ"), Some("DZD"));
        assert_eq!(app_storefront_currency_for_region("DZ"), Some("USD"));
        assert_eq!(currency_for_region("BD"), Some("BDT"));
        assert_eq!(app_storefront_currency_for_region("BD"), Some("USD"));
        assert_eq!(app_storefront_currency_for_region("PK"), Some("PKR"));
        assert_eq!(app_storefront_currency_for_region("BG"), Some("EUR"));
        assert_eq!(currency_for_region("BH"), Some("BHD"));
        assert_eq!(app_storefront_currency_for_region("BH"), Some("USD"));
    }

    #[test]
    fn parses_common_price_formats() {
        assert_price("$11.99", 11.99);
        assert_price("1,99 €", 1.99);
        assert_price("€1.299,99", 1299.99);
        assert_price("Rp79.000", 79000.0);
        assert_price_for_currency("KD 1.234", "KWD", 1.234);
        assert_price_for_currency("৳499.00", "BDT", 499.0);
        assert_price_for_currency("Rp605 rb", "IDR", 605000.0);
        assert_price_for_currency("Rp604ribu", "IDR", 604000.0);
        assert_price_for_currency("Rp604,5 ribu", "IDR", 604500.0);
        assert_price_for_currency("59,99 lei", "RON", 59.99);
        assert_price_for_currency("₺79,99", "TRY", 79.99);
    }

    #[test]
    fn formats_and_keeps_free_prices() {
        let region = Region {
            code: "US",
            name: "United States",
        };
        let entry = pricing_entry(region, 0.0, "USD").expect("free entry should be kept");

        assert_eq!(format_price(entry.amount, &entry.currency), "Free");
        assert_eq!(entry.region, "United States");
        assert_eq!(entry.region_code, "US");
        assert_eq!(format_price(79.99, "TRY"), "₺79.99");
    }

    #[test]
    fn detects_ambiguous_dollar_currency_from_region() {
        assert_eq!(detect_currency_from_string("$4.99", "US"), "USD");
        assert_eq!(detect_currency_from_string("$4.99", "CA"), "CAD");
        assert_eq!(detect_currency_from_string("$4.99", "AU"), "AUD");
        assert_eq!(detect_currency_from_string("$35.99", "DZ"), "USD");
        assert_eq!(detect_currency_from_string("35.99", "DZ"), "USD");
        assert_eq!(detect_currency_from_string("$35.99", "BD"), "USD");
        assert_eq!(detect_currency_from_string("$719.00", "MX"), "MXN");
        assert_eq!(detect_currency_from_string("US$35.99", "SG"), "USD");
        assert_eq!(detect_currency_from_string("CA$46.90", "US"), "CAD");
        assert_eq!(detect_currency_from_string("৳499.00", "BD"), "BDT");
        assert_eq!(detect_currency_from_string("₺79,99", "US"), "TRY");
        assert_eq!(detect_currency_from_string("79,99 TL", "US"), "TRY");
    }

    #[test]
    fn normalizes_display_currency_names_to_iso_codes() {
        assert_eq!(detect_currency_from_string("59,99 lei", "RO"), "RON");
        assert_eq!(detect_currency_from_string("KSh 499.00", "KE"), "KES");
        assert_eq!(detect_currency_from_string("TSh 104900", "TZ"), "TZS");
        assert_eq!(detect_currency_from_string("USh 129000", "UG"), "UGX");
        assert_eq!(detect_currency_from_string("FRw 49900", "RW"), "RWF");
        assert_eq!(detect_currency_from_string("د.ت35.990", "TN"), "TND");
        assert_eq!(detect_currency_from_string("KD 35.990", "KW"), "KWD");
        assert_eq!(detect_currency_from_string("JD 35.990", "JO"), "JOD");
    }

    #[test]
    fn cleans_dangling_iap_parentheticals_for_display_only() {
        assert_eq!(
            display_iap_name("2026 New Year Habit Tracker (4"),
            "2026 New Year Habit Tracker..."
        );
        assert_eq!(
            display_iap_name("The Weekly Action Planner (Sun"),
            "The Weekly Action Planner..."
        );
        assert_eq!(
            display_iap_name("Monthly Project Budget (Green)"),
            "Monthly Project Budget (Green)"
        );
        assert_eq!(
            display_iap_name("Goodnotes 6 Free Trial"),
            "Goodnotes 6 Free Trial"
        );
    }

    #[test]
    fn checks_iap_pricing_only_where_base_app_exists() {
        let sg = Region {
            code: "SG",
            name: "Singapore",
        };
        let us = Region {
            code: "US",
            name: "United States",
        };
        let analysis = AnalysisResult {
            app_id: "1444383602".to_string(),
            display_name: "Goodnotes".to_string(),
            base_region: sg,
            base_currency: "SGD".to_string(),
            base_pricing: vec![
                pricing_entry(us, 0.0, "USD").expect("US should build"),
                pricing_entry(sg, 0.0, "SGD").expect("SG should build"),
            ],
            iaps: Vec::new(),
        };

        let codes = iap_pricing_regions(&analysis)
            .into_iter()
            .map(|region| region.code)
            .collect::<Vec<_>>();
        assert_eq!(codes, vec!["SG", "US"]);
    }

    #[test]
    fn parses_and_deduplicates_iap_rows_without_guessing_categories() {
        let html = r#"
            <div class="text-pair"><span>First Product</span> <span>S$&nbsp;2.98</span></div>
            <div class="text-pair"><span>First Product</span> <span>S$&nbsp;2.98</span></div>
            <div class="text-pair"><span>Second Product</span> <span>S$&nbsp;14.98</span></div>
        "#;

        let iaps = parse_iap_list_from_html(html);
        assert_eq!(iaps.len(), 2);
        assert_eq!(iaps[0].name, "First Product");
        assert_eq!(iaps[0].price_string, "S$\u{a0}2.98");
        assert_eq!(iaps[1].name, "Second Product");
    }

    #[test]
    fn parses_iap_rows_from_serialized_server_data_text_pairs() {
        let html = r#"
            <script type="application/json" id="serialized-server-data">
            {
              "data": [{
                "data": {
                  "shelfMapping": {
                    "information": {
                      "items": [{
                        "$kind": "Annotation",
                        "title": "In-App Purchases",
                        "items": [{
                          "$kind": "AnnotationItem",
                          "textPairs": [
                            ["First Product", "S$&nbsp;2.98"],
                            ["Second Product", "S$&nbsp;14.98"]
                          ]
                        }]
                      }]
                    }
                  }
                }
              }]
            }
            </script>
        "#;

        let iaps = parse_iap_list_from_html(html);
        assert_eq!(iaps.len(), 2);
        assert_eq!(iaps[0].name, "First Product");
        assert_eq!(iaps[0].price_string, "S$\u{a0}2.98");
        assert_eq!(iaps[1].name, "Second Product");
    }

    #[test]
    fn parses_iap_rows_from_serialized_server_data_items_v3() {
        let html = r#"
            <script type="application/json" id="serialized-server-data">
            {
              "data": [{
                "data": {
                  "shelfMapping": {
                    "information": {
                      "items": [{
                        "$kind": "Annotation",
                        "title": "In-App Purchases",
                        "items_V3": [
                          {
                            "$kind": "textPair",
                            "leadingText": "First Product",
                            "trailingText": "$1.99"
                          },
                          {
                            "$kind": "button",
                            "action": {"title": "Learn More"}
                          }
                        ]
                      }]
                    }
                  }
                }
              }]
            }
            </script>
        "#;

        let iaps = parse_iap_list_from_html(html);
        assert_eq!(iaps.len(), 1);
        assert_eq!(iaps[0].name, "First Product");
        assert_eq!(iaps[0].price_string, "$1.99");
    }

    #[test]
    fn parses_iap_rows_from_iap_section_only_when_section_is_present() {
        let html = r#"
            <div class="text-pair"><span>Age Rating</span> <span>4+</span></div>
            <dt>In-App Purchases</dt>
            <dd>
                <div class="text-pair"><span>First Product</span> <span>$1.99</span></div>
            </dd>
        "#;

        let iaps = parse_iap_list_from_html(html);
        assert_eq!(iaps.len(), 1);
        assert_eq!(iaps[0].name, "First Product");
    }

    #[test]
    fn parses_apple_country_picker_aliases_without_confusing_site_routes() {
        let html = r#"
            <a href="/la/">
                <meta property="schema:inLanguage" content="es-419" />
                <span class="countrylist-caption block-link" lang="es-419">América Latina y el Caribe (Español)</span>
            </a>
            <a href="/la/">
                <meta property="schema:inLanguage" content="es-VE" />
                <span class="countrylist-caption block-link" lang="es-VE">Venezuela</span>
            </a>
            <a href="/la/">
                <meta property="schema:inLanguage" content="es-UY" />
                <span class="countrylist-caption block-link" lang="es-UY">Uruguay</span>
            </a>
            <a href="/vn/">
                <meta property="schema:inLanguage" content="vi-VN" />
                <span class="countrylist-caption block-link" lang="vi-VN">Việt Nam</span>
            </a>
            <a href="/ru/">
                <span class="countrylist-caption block-link">Россия</span>
            </a>
            <a href="/bh/">
                <meta property="schema:inLanguage" content="en-BH" />
                <span class="countrylist-caption block-link" lang="en-BH">Bahrain</span>
            </a>
            <a href="/gi/">
                <meta property="schema:inLanguage" content="en-GI" />
                <span class="countrylist-caption block-link" lang="en-GI">Gibraltar</span>
            </a>
        "#;

        let entries = parse_apple_country_entries(html);
        assert!(entries
            .iter()
            .any(|entry| entry.site_code == "la" && entry.inferred_code.as_deref() == Some("VE")));
        assert!(entries
            .iter()
            .any(|entry| entry.site_code == "la" && entry.inferred_code.as_deref() == Some("UY")));
        assert!(
            entries
                .iter()
                .any(|entry| entry.label == "Việt Nam"
                    && entry.inferred_code.as_deref() == Some("VN"))
        );
        assert!(entries
            .iter()
            .any(|entry| entry.label == "Россия" && entry.inferred_code.as_deref() == Some("RU")));
        assert!(entries
            .iter()
            .any(|entry| entry.label.contains("América") && entry.inferred_code.is_none()));

        let report = build_region_audit_report(html);
        assert_eq!(
            report
                .route_aliases_by_region
                .get("VE")
                .expect("VE should have LA alias"),
            &vec!["LA".to_string()]
        );
        assert_eq!(
            report
                .missing_candidates
                .iter()
                .map(|candidate| candidate.code.as_str())
                .collect::<Vec<_>>(),
            vec!["GI"]
        );
    }

    #[test]
    fn region_codes_are_unique() {
        let mut seen = std::collections::HashSet::new();
        for region in REGIONS {
            assert!(seen.insert(region.code), "duplicate region {}", region.code);
        }
    }
}
