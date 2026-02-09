"""Configuration for ptars protobuf to Arrow conversions."""

from dataclasses import dataclass
from typing import Any, Literal

TimeUnitLiteral = Literal["s", "ms", "us", "ns"]


_VALID_TIME_UNITS = {"s", "ms", "us", "ns"}


def _check_type(value: Any, expected_type: type, name: str) -> None:
    """Raise TypeError if value is not of expected type."""
    if not isinstance(value, expected_type):
        raise TypeError(
            f"{name} must be {expected_type.__name__}, got {type(value).__name__}"
        )


def _check_time_unit(value: str, name: str) -> None:
    """Raise ValueError if value is not a valid time unit."""
    if value not in _VALID_TIME_UNITS:
        raise ValueError(f"{name} must be one of {_VALID_TIME_UNITS}, got {value!r}")


@dataclass(frozen=True)
class PtarsConfig:
    """Configuration for protobuf to Arrow conversions.

    Attributes:
        timestamp_tz: Timezone for timestamp values. Use None for timezone-naive.
        timestamp_unit: Time unit for timestamps ("s", "ms", "us", "ns").
        time_unit: Time unit for time of day ("s", "ms", "us", "ns").
        duration_unit: Time unit for durations ("s", "ms", "us", "ns").
        list_value_name: Field name for list items in Arrow schema.
        list_nullable: Whether list fields can be null.
        map_nullable: Whether map fields can be null.
        list_value_nullable: Whether list elements can be null.
        map_value_nullable: Whether map values can be null.
        use_large_string: Whether to use LargeUtf8 instead of Utf8 for strings.
        use_large_binary: Whether to use LargeBinary instead of Binary for bytes.
        use_large_list: Whether to use LargeList instead of List for repeated fields.

    Note:
        The Rust API also supports `map_value_name` for customizing the field name
        of map values in the Arrow schema. This option is not exposed in Python
        because Arrow's MapType uses a fixed "value" field name when constructed
        via PyArrow's C data interface. The default "value" name is always used
        for map values in Python.
    """

    timestamp_tz: str | None = "UTC"
    timestamp_unit: TimeUnitLiteral = "ns"
    time_unit: TimeUnitLiteral = "ns"
    duration_unit: TimeUnitLiteral = "ns"
    list_value_name: str = "item"
    list_nullable: bool = False
    map_nullable: bool = False
    list_value_nullable: bool = False
    map_value_nullable: bool = False
    use_large_string: bool = False
    use_large_binary: bool = False
    use_large_list: bool = False

    def __post_init__(self) -> None:
        """Validate configuration values."""
        if self.timestamp_tz is not None:
            _check_type(self.timestamp_tz, str, "timestamp_tz")
        _check_time_unit(self.timestamp_unit, "timestamp_unit")
        _check_time_unit(self.time_unit, "time_unit")
        _check_time_unit(self.duration_unit, "duration_unit")
        _check_type(self.list_value_name, str, "list_value_name")
        _check_type(self.list_nullable, bool, "list_nullable")
        _check_type(self.map_nullable, bool, "map_nullable")
        _check_type(self.list_value_nullable, bool, "list_value_nullable")
        _check_type(self.map_value_nullable, bool, "map_value_nullable")
        _check_type(self.use_large_string, bool, "use_large_string")
        _check_type(self.use_large_binary, bool, "use_large_binary")
        _check_type(self.use_large_list, bool, "use_large_list")
