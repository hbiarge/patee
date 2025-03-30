from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path
from typing import Union

from patee.input_types import MonolingualSingleFilePair, MultilingualSingleFile, MonolingualSingleFile


class Step(ABC):
    """Base class for all extraction steps."""

    def __init__(self, name: str):
        """Initialize the step."""
        self.name = name


@dataclass(frozen=True)
class LanguageResultSource:
    document_path: Path
    iso2_language: str

    @staticmethod
    def from_monolingual_file(file: MonolingualSingleFile) -> 'LanguageResultSource':
        return LanguageResultSource(file.document_path, file.iso2_language)

    @staticmethod
    def from_multilingual_file(file: MultilingualSingleFile, language_idx: int) -> 'LanguageResultSource':
        return LanguageResultSource(file.document_path, file.iso2_languages[language_idx])


@dataclass(frozen=True)
class LanguageResult:
    source: LanguageResultSource
    text: str
    extra: dict

    def write_to_file(self, result_dir: Path):
        file_path = result_dir / f"{self.source.document_path.stem}_{self.source.iso2_language}.txt"
        file_path.write_text(self.text)

        if len(self.extra) > 0:
            extra_path = result_dir / f"{self.source.document_path.stem}_{self.source.iso2_language}_extra.json"
            extra_path.write_text(str(self.extra))


@dataclass(frozen=True)
class StepResult:
    document_1: LanguageResult
    document_2: LanguageResult

    def write_to_files(self, out_dir: Path) -> None:
        self.document_1.write_to_file(out_dir)
        self.document_2.write_to_file(out_dir)


class ParallelExtractStep(Step):
    """Base class for all extraction steps."""

    def __init__(self, name: str):
        """Initialize the step."""
        super().__init__(name)

    @abstractmethod
    def extract(self, source: Union[MonolingualSingleFilePair, MultilingualSingleFile]) -> StepResult:
        """Extract data from a source."""
        pass


class ParallelProcessStep(Step):
    """Base class for all extraction steps."""

    def __init__(self, name: str):
        """Initialize the step."""
        super().__init__(name)

    @abstractmethod
    def process(self, source: StepResult) -> StepResult:
        """Extract data from a source."""
        pass



