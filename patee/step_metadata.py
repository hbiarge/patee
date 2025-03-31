from dataclasses import dataclass


@dataclass(frozen=True)
class StepMetadata:
    name: str
    type: str
    idx: int
    config: dict
