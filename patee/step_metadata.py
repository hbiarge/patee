from dataclasses import dataclass


@dataclass(frozen=True)
class StepMetadata:
    name: str
    type: str
    idx: int
    config: dict

    def __key(self):
        return self.name, self.type, self.idx, self.config

    def __hash__(self):
        return hash(self.__key())

    def __eq__(self, other):
        if isinstance(other, StepMetadata):
            return self.__key() == other.__key()
        return NotImplemented
