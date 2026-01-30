"""Configuration for ptars protobuf to Arrow conversions."""

from dataclasses import dataclass
from typing import Literal

TimeUnitLiteral = Literal["s", "ms", "us", "ns"]


_VALID_TIME_UNITS = {"s", "ms", "us", "ns"}


@dataclass(frozen=True)
class PtarsConfig:
    """Configuration for protobuf to Arrow conversions.

    Attributes:
        timestamp_tz: Timezone for timestamp values. Use None for timezone-naive.
        timestamp_unit: Time unit for timestamps ("s", "ms", "us", "ns").
        time_unit: Time unit for time of day ("s", "ms", "us", "ns").
        duration_unit: Time unit for durations ("s", "ms", "us", "ns").
        list_value_name: Field name for list items in Arrow schema.
        map_value_name: Field name for map values in Arrow schema.
        list_nullable: Whether list fields can be null.
        map_nullable: Whether map fields can be null.
        list_value_nullable: Whether list elements can be null.
        map_value_nullable: Whether map values can be null.
    """

    timestamp_tz: str | None = "UTC"
    timestamp_unit: TimeUnitLiteral = "ns"
    time_unit: TimeUnitLiteral = "ns"
    duration_unit: TimeUnitLiteral = "ns"
    list_value_name: str = "item"
    map_value_name: str = "value"
    list_nullable: bool = False
    map_nullable: bool = False
    list_value_nullable: bool = False
    map_value_nullable: bool = False

    def __post_init__(self) -> None:
        """Validate configuration values."""
        if self.timestamp_tz is not None and not isinstance(self.timestamp_tz, str):
            raise TypeError(
                f"timestamp_tz must be str or None, got {type(self.timestamp_tz).__name__}"
            )
        if self.timestamp_unit not in _VALID_TIME_UNITS:
            raise ValueError(
                f"timestamp_unit must be one of {_VALID_TIME_UNITS}, got {self.timestamp_unit!r}"
            )
        if self.time_unit not in _VALID_TIME_UNITS:
            raise ValueError(
                f"time_unit must be one of {_VALID_TIME_UNITS}, got {self.time_unit!r}"
            )
        if self.duration_unit not in _VALID_TIME_UNITS:
            raise ValueError(
                f"duration_unit must be one of {_VALID_TIME_UNITS}, got {self.duration_unit!r}"
            )
        if not isinstance(self.list_value_name, str):
            raise TypeError(
                f"list_value_name must be str, got {type(self.list_value_name).__name__}"
            )
        if not isinstance(self.map_value_name, str):
            raise TypeError(
                f"map_value_name must be str, got {type(self.map_value_name).__name__}"
            )
        if not isinstance(self.list_nullable, bool):
            raise TypeError(
                f"list_nullable must be bool, got {type(self.list_nullable).__name__}"
            )
        if not isinstance(self.map_nullable, bool):
            raise TypeError(
                f"map_nullable must be bool, got {type(self.map_nullable).__name__}"
            )
        if not isinstance(self.list_value_nullable, bool):
            raise TypeError(
                f"list_value_nullable must be bool, got {type(self.list_value_nullable).__name__}"
            )
        if not isinstance(self.map_value_nullable, bool):
            raise TypeError(
                f"map_value_nullable must be bool, got {type(self.map_value_nullable).__name__}"
            )
