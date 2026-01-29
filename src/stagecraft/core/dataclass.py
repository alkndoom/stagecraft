"""Automatic dataclass utilities for creating immutable, slotted dataclasses.

This module provides utilities for creating efficient, immutable dataclasses with
minimal boilerplate. It combines Python's standard dataclass functionality with
additional features like slots and frozen instances for better performance and safety.

The module exports:
    - autodataclass: Decorator that creates an immutable, slotted dataclass
    - AutoDataClass: Base class for dataclasses with serialization support

Example:
    >>> from stagecraft.core.dataclass import autodataclass, AutoDataClass
    >>>
    >>> @autodataclass
    >>> class Point:
    ...     x: float
    ...     y: float
    ...
    >>> p = Point(x=1.0, y=2.0)
    >>> p.x = 3.0  # Raises FrozenInstanceError
    >>>
    >>> class Config(AutoDataClass):
    ...     host: str
    ...     port: int
    ...
    >>> config = Config(host="localhost", port=8080)
    >>> config.to_dict()
    {'host': 'localhost', 'port': 8080}
"""

from dataclasses import dataclass
from typing import Type, TypeVar

from typing_extensions import dataclass_transform

_T = TypeVar("_T")

_INST = TypeVar("_INST", bound="AutoDataClass")


@dataclass_transform(kw_only_default=True, field_specifiers=())
def autodataclass(cls: Type[_T]) -> Type[_T]:
    """Decorator that creates an immutable, slotted, keyword-only dataclass.

    This decorator applies Python's @dataclass with optimized settings:
    - slots=True: Uses __slots__ for memory efficiency and faster attribute access
    - frozen=True: Makes instances immutable after creation
    - kw_only=True: Requires all arguments to be passed as keyword arguments

    The @dataclass_transform decorator provides IDE support for type checking
    and autocomplete, ensuring that type checkers understand the resulting class
    will have an __init__ method with parameters matching the class attributes.

    Args:
        cls: The class to transform into a dataclass.

    Returns:
        The same class, transformed into an immutable, slotted dataclass.

    Example:
        >>> @autodataclass
        >>> class Person:
        ...     name: str
        ...     age: int
        ...     email: str = "unknown@example.com"
        ...
        >>> person = Person(name="Alice", age=30)
        >>> person.name
        'Alice'
        >>> person.age = 31  # Raises FrozenInstanceError
        >>> person.email
        'unknown@example.com'

    Note:
        - All instances are immutable (frozen) after creation
        - Memory usage is reduced compared to regular classes due to __slots__
        - All constructor arguments must be passed as keyword arguments
        - Inheritance from slotted classes requires careful handling of __slots__
    """
    return dataclass(cls, slots=True, frozen=True, kw_only=True)


@autodataclass
class AutoDataClass:
    """Base class for immutable dataclasses with serialization support.

    AutoDataClass provides a foundation for creating immutable, memory-efficient
    dataclasses with built-in dictionary serialization. It automatically applies
    the autodataclass decorator, making all subclasses frozen and slotted.

    This class is particularly useful for:
    - Configuration objects that should not be modified after creation
    - Data transfer objects (DTOs) with serialization needs
    - Schema definitions that require immutability guarantees
    - Value objects in domain-driven design

    The class provides a to_dict() method that serializes all attributes to a
    dictionary, which is useful for JSON serialization, logging, or debugging.

    Attributes:
        All attributes are defined by subclasses and are immutable after initialization.

    Example:
        >>> @autodataclass
        >>> class DatabaseConfig(AutoDataClass):
        ...     host: str
        ...     port: int
        ...     database: str
        ...     username: str
        ...     password: str
        ...     ssl_enabled: bool = True
        ...
        >>> config = DatabaseConfig(
        ...     host="localhost",
        ...     port=5432,
        ...     database="mydb",
        ...     username="admin",
        ...     password="secret"
        ... )
        >>> config.to_dict()
        {
            'host': 'localhost',
            'port': 5432,
            'database': 'mydb',
            'username': 'admin',
            'password': 'secret',
            'ssl_enabled': True
        }
        >>> config.port = 3306  # Raises FrozenInstanceError

    Example with inheritance:
        >>> @autodataclass
        >>> class BaseSchema(AutoDataClass):
        ...     id: int
        ...     created_at: str
        ...
        >>> @autodataclass
        >>> class UserSchema(BaseSchema):
        ...     username: str
        ...     email: str
        ...
        >>> user = UserSchema(
        ...     id=1,
        ...     created_at="2026-01-29",
        ...     username="alice",
        ...     email="alice@example.com"
        ... )
        >>> user.to_dict()
        {
            'id': 1,
            'created_at': '2026-01-29',
            'username': 'alice',
            'email': 'alice@example.com'
        }

    Note:
        - Instances are immutable (frozen) and cannot be modified after creation
        - Uses __slots__ for memory efficiency
        - All constructor arguments must be keyword arguments
        - The to_dict() method uses __slots__ for efficient serialization
    """

    def to_dict(self) -> dict:
        """Serialize the dataclass instance to a dictionary.

        Converts all attributes of the dataclass to a dictionary representation.
        This method iterates through __slots__ to efficiently extract all field
        values without relying on __dict__.

        Returns:
            A dictionary mapping attribute names to their values.

        Example:
            >>> @autodataclass
            >>> class Point(AutoDataClass):
            ...     x: float
            ...     y: float
            ...     label: str = "origin"
            ...
            >>> point = Point(x=1.5, y=2.5)
            >>> point.to_dict()
            {'x': 1.5, 'y': 2.5, 'label': 'origin'}

        Note:
            - This method is efficient for slotted classes
            - Nested AutoDataClass instances are not automatically serialized
            - For complex nested structures, consider implementing custom serialization
        """
        return {name: getattr(self, name) for name in self.__slots__}  # type: ignore
