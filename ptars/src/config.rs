use arrow_schema::TimeUnit;
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

    /// Time unit for timestamp values. Default: Nanosecond
    pub timestamp_unit: TimeUnit,

    /// Time unit for time of day values. Default: Nanosecond
    pub time_unit: TimeUnit,

    /// Time unit for duration values. Default: Nanosecond
    pub duration_unit: TimeUnit,

    /// Name for list item field. Default: "item"
    pub list_value_name: Arc<str>,

    /// Name for map value field. Default: "value"
    pub map_value_name: Arc<str>,

    /// Whether list fields can be null. Default: false
    pub list_nullable: bool,

    /// Whether map fields can be null. Default: false
    pub map_nullable: bool,

    /// Whether list element values can be null. Default: false
    pub list_value_nullable: bool,

    /// Whether map values can be null. Default: false
    pub map_value_nullable: bool,

    /// Whether to use LargeUtf8 instead of Utf8 for string fields. Default: false
    pub use_large_string: bool,

    /// Whether to use LargeBinary instead of Binary for bytes fields. Default: false
    pub use_large_binary: bool,
}

impl Default for PtarsConfig {
    fn default() -> Self {
        Self {
            timestamp_tz: Some(Arc::from("UTC")),
            timestamp_unit: TimeUnit::Nanosecond,
            time_unit: TimeUnit::Nanosecond,
            duration_unit: TimeUnit::Nanosecond,
            list_value_name: Arc::from("item"),
            map_value_name: Arc::from("value"),
            list_nullable: false,
            map_nullable: false,
            list_value_nullable: false,
            map_value_nullable: false,
            use_large_string: false,
            use_large_binary: false,
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

    /// Set the time unit for timestamp values.
    pub fn with_timestamp_unit(mut self, unit: TimeUnit) -> Self {
        self.timestamp_unit = unit;
        self
    }

    /// Set the time unit for time of day values.
    pub fn with_time_unit(mut self, unit: TimeUnit) -> Self {
        self.time_unit = unit;
        self
    }

    /// Set the time unit for duration values.
    pub fn with_duration_unit(mut self, unit: TimeUnit) -> Self {
        self.duration_unit = unit;
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

    /// Set whether list fields can be null.
    pub fn with_list_nullable(mut self, nullable: bool) -> Self {
        self.list_nullable = nullable;
        self
    }

    /// Set whether map fields can be null.
    pub fn with_map_nullable(mut self, nullable: bool) -> Self {
        self.map_nullable = nullable;
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

    /// Set whether to use LargeUtf8 instead of Utf8 for string fields.
    pub fn with_use_large_string(mut self, use_large: bool) -> Self {
        self.use_large_string = use_large;
        self
    }

    /// Set whether to use LargeBinary instead of Binary for bytes fields.
    pub fn with_use_large_binary(mut self, use_large: bool) -> Self {
        self.use_large_binary = use_large;
        self
    }
}
