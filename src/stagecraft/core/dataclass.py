from dataclasses import dataclass
from typing import Type, TypeVar

from typing_extensions import dataclass_transform

_T = TypeVar("_T")

_INST = TypeVar("_INST", bound="AutoDataClass")


@dataclass_transform(kw_only_default=True, field_specifiers=())
def autodataclass(cls: Type[_T]) -> Type[_T]:
    return dataclass(cls, slots=True, frozen=True, kw_only=True)


@autodataclass
class AutoDataClass:

    def to_dict(self) -> dict:
        return {name: getattr(self, name) for name in self.__slots__}  # type: ignore
