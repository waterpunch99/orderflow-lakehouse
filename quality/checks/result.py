from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class CheckResult:
    name: str
    status: str
    observed_value: int | float | str
    expected: str
    details: str

    @property
    def failed(self) -> bool:
        return self.status == "FAIL"

    def format(self) -> str:
        return (
            f"[{self.status}] {self.name} "
            f"observed={self.observed_value} expected={self.expected} details={self.details}"
        )
