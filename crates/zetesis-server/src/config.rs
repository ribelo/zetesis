use std::net::IpAddr;
use std::num::{NonZeroU32, NonZeroU64};

use axum::http::{Method, header::HeaderName};
use serde::Deserialize;

#[derive(Debug, Deserialize, Clone)]
pub struct ServerConfig {
    pub listen_addr: String,
    #[serde(default)]
    pub rate_limit: RateLimitConfig,
    #[serde(default)]
    pub cors: CorsConfig,
}

#[derive(Debug, Deserialize, Clone)]
pub struct RateLimitConfig {
    #[serde(default = "RateLimitConfig::default_enabled")]
    pub enabled: bool,
    #[serde(default = "RateLimitConfig::default_window_ms")]
    pub window_ms: NonZeroU64,
    #[serde(default = "RateLimitConfig::default_keyword_limit")]
    pub keyword: RouteLimitConfig,
    #[serde(default = "RateLimitConfig::default_vector_limit")]
    pub vector: RouteLimitConfig,
    #[serde(default = "RateLimitConfig::default_typeahead_limit")]
    pub typeahead: RouteLimitConfig,
    #[serde(default = "RateLimitConfig::default_hybrid_limit")]
    pub hybrid: RouteLimitConfig,
    #[serde(default)]
    pub proxy_mode: ProxyMode,
    #[serde(default)]
    pub trusted_proxies: Vec<IpAddr>,
}

impl RateLimitConfig {
    fn default_enabled() -> bool {
        true
    }

    fn default_window_ms() -> NonZeroU64 {
        NonZeroU64::new(1_000).expect("non-zero window defaults to 1000ms")
    }

    fn default_keyword_limit() -> RouteLimitConfig {
        RouteLimitConfig::keyword_defaults()
    }

    fn default_vector_limit() -> RouteLimitConfig {
        RouteLimitConfig::vector_defaults()
    }

    fn default_typeahead_limit() -> RouteLimitConfig {
        RouteLimitConfig::typeahead_defaults()
    }

    fn default_hybrid_limit() -> RouteLimitConfig {
        RouteLimitConfig::hybrid_defaults()
    }
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            enabled: Self::default_enabled(),
            window_ms: Self::default_window_ms(),
            keyword: Self::default_keyword_limit(),
            vector: Self::default_vector_limit(),
            typeahead: Self::default_typeahead_limit(),
            hybrid: Self::default_hybrid_limit(),
            proxy_mode: ProxyMode::Off,
            trusted_proxies: Vec::new(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct CorsConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub allow_origins: Vec<String>,
    #[serde(default = "CorsConfig::default_allow_methods")]
    pub allow_methods: Vec<String>,
    #[serde(default = "CorsConfig::default_allow_headers")]
    pub allow_headers: Vec<String>,
    #[serde(default)]
    pub expose_headers: Vec<String>,
    #[serde(default)]
    pub allow_credentials: bool,
    #[serde(default = "CorsConfig::default_max_age_secs")]
    pub max_age_secs: u64,
}

impl CorsConfig {
    fn default_allow_methods() -> Vec<String> {
        vec!["GET".to_string(), "OPTIONS".to_string()]
    }

    fn default_allow_headers() -> Vec<String> {
        vec!["authorization".to_string(), "content-type".to_string()]
    }

    fn default_max_age_secs() -> u64 {
        600
    }
}

impl Default for CorsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            allow_origins: Vec::new(),
            allow_methods: Self::default_allow_methods(),
            allow_headers: Self::default_allow_headers(),
            expose_headers: Vec::new(),
            allow_credentials: false,
            max_age_secs: Self::default_max_age_secs(),
        }
    }
}

#[derive(Debug, Deserialize, Clone)]
pub struct RouteLimitConfig {
    #[serde(default = "RouteLimitConfig::default_requests")]
    pub max_requests: NonZeroU32,
    #[serde(default = "RouteLimitConfig::default_burst")]
    pub burst: NonZeroU32,
}

impl RouteLimitConfig {
    fn default_requests() -> NonZeroU32 {
        NonZeroU32::new(1).expect("default requests bound must be non-zero")
    }

    fn default_burst() -> NonZeroU32 {
        NonZeroU32::new(1).expect("default burst bound must be non-zero")
    }

    fn keyword_defaults() -> Self {
        Self {
            max_requests: NonZeroU32::new(10).expect("keyword default must be non-zero"),
            burst: NonZeroU32::new(20).expect("keyword burst must be non-zero"),
        }
    }

    fn vector_defaults() -> Self {
        Self {
            max_requests: NonZeroU32::new(3).expect("vector default must be non-zero"),
            burst: NonZeroU32::new(6).expect("vector burst must be non-zero"),
        }
    }

    fn typeahead_defaults() -> Self {
        Self {
            max_requests: NonZeroU32::new(20).expect("typeahead default must be non-zero"),
            burst: NonZeroU32::new(40).expect("typeahead burst must be non-zero"),
        }
    }

    fn hybrid_defaults() -> Self {
        Self {
            max_requests: NonZeroU32::new(2).expect("hybrid default must be non-zero"),
            burst: NonZeroU32::new(4).expect("hybrid burst must be non-zero"),
        }
    }
}

impl Default for RouteLimitConfig {
    fn default() -> Self {
        Self {
            max_requests: Self::default_requests(),
            burst: Self::default_burst(),
        }
    }
}

#[derive(Debug, Deserialize, Clone, Copy, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ProxyMode {
    Off,
    XForwardedFor,
    Forwarded,
}

impl Default for ProxyMode {
    fn default() -> Self {
        ProxyMode::Off
    }
}

pub fn parse_method(method: &str) -> Result<Method, String> {
    Method::from_bytes(method.as_bytes())
        .map_err(|_| format!("invalid HTTP method `{method}` in CORS allow_methods"))
}

pub fn parse_header(name: &str) -> Result<HeaderName, String> {
    HeaderName::from_bytes(name.as_bytes())
        .map_err(|_| format!("invalid HTTP header `{name}` in CORS configuration"))
}
