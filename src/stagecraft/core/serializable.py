import enum
from datetime import datetime
from typing import Any, Dict, Mapping, Protocol, Type, TypeVar

import numpy as np
import pandas as pd
from pydantic import BaseModel, ConfigDict, field_serializer

from .str import snake_to_camel_case

_SERIALIZABLE = TypeVar("_SERIALIZABLE", bound="Serializable")


class DictSerializable(Protocol):
    def to_dict(self, *, convert_keys: bool = False) -> Dict[str, Any]: ...


class Serializable(BaseModel):
    model_config = ConfigDict(
        alias_generator=snake_to_camel_case,
        populate_by_name=True,
        arbitrary_types_allowed=True,
        ser_json_timedelta="iso8601",
        ser_json_bytes="base64",
    )

    @field_serializer("*", check_fields=False)
    def serialize_all_types(self, value):
        """Serialize various types to JSON-compatible formats."""
        if isinstance(value, datetime):
            return value.strftime("%Y-%m-%d %H:%M:%S")
        elif isinstance(value, pd.DataFrame):
            return value.to_dict("records")
        elif isinstance(value, pd.Series):
            return value.to_list()
        elif isinstance(value, np.ndarray):
            return value.tolist()
        elif isinstance(value, enum.Enum):
            return value.value
        elif hasattr(value, "to_dict") and callable(value.to_dict):
            return value.to_dict()
        elif isinstance(value, object):
            return str(value)
        return value

    def to_dict(self, *, convert_keys: bool = False) -> Dict[str, Any]:
        """
        Serialize object to Python dictionary.

        Args:
            convert_keys: Convert keys to camelCase (JSON standard)
        """
        if convert_keys:
            return self.model_dump(by_alias=True)
        return self.model_dump(by_alias=False)

    @classmethod
    def from_dict(
        cls: Type[_SERIALIZABLE],
        data: Mapping[str, Any],
        *,
        convert_keys: bool = False,
    ) -> _SERIALIZABLE:
        """
        Deserialize object from Python dictionary.

        Args:
            data: Dictionary to deserialize from
            convert_keys: Convert keys from camelCase (JSON standard)
        """
        return cls.model_validate(data, by_alias=convert_keys, by_name=True)


class FrozenSerializable(Serializable):
    """Immutable version of Serializable."""

    model_config = ConfigDict(
        frozen=True,
        alias_generator=snake_to_camel_case,
        populate_by_name=True,
        arbitrary_types_allowed=True,
        ser_json_timedelta="iso8601",
        ser_json_bytes="base64",
    )
