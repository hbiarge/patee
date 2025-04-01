import logging
from typing import Union

from patee.input_types import MonolingualSingleFilePair, MultilingualSingleFile
from patee.steps import ParallelExtractStep, StepResult, LanguageResult, LanguageResultSource

logger = logging.getLogger(__name__)


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
        logger.debug("reading document 1 from %s ...", source.document_1.document_path)
        document_1_text = source.document_1.document_path.read_text(encoding="utf-8")

        logger.debug("reading document 2 from %s ...", source.document_2.document_path)
        document_2_text = source.document_2.document_path.read_text(encoding="utf-8")

        result = StepResult(
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

        logger.debug("monolingual single file pairs read successfully.")

        return result

    def _extract_single_file(self, source: MultilingualSingleFile) -> StepResult:
        raise NotImplementedError("Single file extraction is not implemented yet.")
