use std::sync::Arc;

/// Configuration for protobuf to Arrow conversions.
///
/// This struct allows customizing how protobuf types are mapped to Arrow types,
/// similar to Python's `ProtarrowConfig`.
#[derive(Debug, Clone)]
pub struct PtarsConfig {
    /// Timezone for timestamp values. Default: Some("UTC")
    /// Set to None for timezone-naive timestamps.
    pub timestamp_tz: Option<Arc<str>>,

    /// Name for list item field. Default: "item"
    pub list_value_name: Arc<str>,

    /// Name for map value field. Default: "value"
    pub map_value_name: Arc<str>,

    /// Whether list element values can be null. Default: false
    pub list_value_nullable: bool,

    /// Whether map values can be null. Default: false
    pub map_value_nullable: bool,
}

impl Default for PtarsConfig {
    fn default() -> Self {
        Self {
            timestamp_tz: Some(Arc::from("UTC")),
            list_value_name: Arc::from("item"),
            map_value_name: Arc::from("value"),
            list_value_nullable: false,
            map_value_nullable: false,
        }
    }
}

impl PtarsConfig {
    /// Create a new config with default values.
    pub fn new() -> Self {
        Self::default()
    }

    /// Set the timezone for timestamp values.
    pub fn with_timestamp_tz(mut self, tz: Option<&str>) -> Self {
        self.timestamp_tz = tz.map(Arc::from);
        self
    }

    /// Set the name for list item fields.
    pub fn with_list_value_name(mut self, name: &str) -> Self {
        self.list_value_name = Arc::from(name);
        self
    }

    /// Set the name for map value fields.
    pub fn with_map_value_name(mut self, name: &str) -> Self {
        self.map_value_name = Arc::from(name);
        self
    }

    /// Set whether list element values can be null.
    pub fn with_list_value_nullable(mut self, nullable: bool) -> Self {
        self.list_value_nullable = nullable;
        self
    }

    /// Set whether map values can be null.
    pub fn with_map_value_nullable(mut self, nullable: bool) -> Self {
        self.map_value_nullable = nullable;
        self
    }
}
