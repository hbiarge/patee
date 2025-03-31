from typing import Union

from patee.input_types import MonolingualSingleFilePair, MultilingualSingleFile
from patee.steps import ParallelExtractStep, StepResult, LanguageResult, LanguageResultSource


class TextReaderExtractor(ParallelExtractStep):
    def __init__(self, name: str, **kwargs):
        super().__init__(name)

    def extract(self, source: Union[MonolingualSingleFilePair, MultilingualSingleFile]) -> StepResult:
        if isinstance(source, MonolingualSingleFilePair):
            return self._extract_file_pair(source)
        elif isinstance(source, MultilingualSingleFile):
            return self._extract_single_file(source)
        else:
            raise ValueError(f"Unsupported source type: {type(source)}")

    def _extract_file_pair(self, source: MonolingualSingleFilePair) -> StepResult:
        document_1_text = source.document_1.document_path.read_text(encoding="utf-8")
        document_2_text = source.document_2.document_path.read_text(encoding="utf-8")

        return StepResult(
            document_1=LanguageResult(
                source=LanguageResultSource.from_monolingual_file(source.document_1),
                text=document_1_text,
                extra={}
            ),
            document_2=LanguageResult(
                source=LanguageResultSource.from_monolingual_file(source.document_2),
                text=document_2_text,
                extra={}
            ),
        )

    def _extract_single_file(self, source: MultilingualSingleFile) -> StepResult:
        raise NotImplementedError("Single file extraction is not implemented yet.")
