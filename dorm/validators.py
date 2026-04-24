"""Built-in validators for dorm fields."""
from __future__ import annotations

import re
from typing import Any

from .exceptions import ValidationError


class MinValueValidator:
    def __init__(self, limit_value: Any) -> None:
        self.limit_value = limit_value

    def __call__(self, value: Any) -> None:
        if value < self.limit_value:
            raise ValidationError(
                f"Ensure this value is greater than or equal to {self.limit_value}."
            )

    def __repr__(self) -> str:
        return f"MinValueValidator({self.limit_value!r})"


class MaxValueValidator:
    def __init__(self, limit_value: Any) -> None:
        self.limit_value = limit_value

    def __call__(self, value: Any) -> None:
        if value > self.limit_value:
            raise ValidationError(
                f"Ensure this value is less than or equal to {self.limit_value}."
            )

    def __repr__(self) -> str:
        return f"MaxValueValidator({self.limit_value!r})"


class MinLengthValidator:
    def __init__(self, min_length: int) -> None:
        self.min_length = min_length

    def __call__(self, value: Any) -> None:
        if len(value) < self.min_length:
            raise ValidationError(
                f"Ensure this value has at least {self.min_length} characters "
                f"(it has {len(value)})."
            )

    def __repr__(self) -> str:
        return f"MinLengthValidator({self.min_length!r})"


class MaxLengthValidator:
    def __init__(self, max_length: int) -> None:
        self.max_length = max_length

    def __call__(self, value: Any) -> None:
        if len(value) > self.max_length:
            raise ValidationError(
                f"Ensure this value has at most {self.max_length} characters "
                f"(it has {len(value)})."
            )

    def __repr__(self) -> str:
        return f"MaxLengthValidator({self.max_length!r})"


class RegexValidator:
    def __init__(self, regex: str, message: str | None = None, flags: int = 0) -> None:
        self.regex = re.compile(regex, flags)
        self.message = message or f"Enter a valid value matching pattern '{regex}'."

    def __call__(self, value: Any) -> None:
        if not self.regex.search(str(value)):
            raise ValidationError(self.message)

    def __repr__(self) -> str:
        return f"RegexValidator(regex={self.regex.pattern!r})"


_EMAIL_RE = re.compile(
    r"^[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}$"
)


class EmailValidator:
    message = "Enter a valid email address."

    def __call__(self, value: Any) -> None:
        if not _EMAIL_RE.match(str(value)):
            raise ValidationError(self.message)

    def __repr__(self) -> str:
        return "EmailValidator()"


validate_email = EmailValidator()
