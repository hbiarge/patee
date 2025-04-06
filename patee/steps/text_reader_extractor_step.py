import logging
from typing import Union

from patee.core_types import PipelineContext
from patee.input_types import MonolingualSingleFilePair, MultilingualSingleFile
from patee.step_types import (
    ParallelExtractStep,
    StepResult,
    DocumentContext,
    DocumentSource,
    StepContext,
    DocumentPairContext,
)


logger = logging.getLogger(__name__)


class TextReaderExtractor(ParallelExtractStep):
    def __init__(self, name: str, pipeline_context: PipelineContext, **kwargs):
        super().__init__(name, pipeline_context)

    @staticmethod
    def step_type() -> str:
        return "text_reader_extractor"

    def extract(self, context: StepContext,
                source: Union[MonolingualSingleFilePair, MultilingualSingleFile]) -> StepResult:
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

        context = DocumentPairContext(
            document_1=DocumentContext(
                source=DocumentSource.from_monolingual_file(source.document_1),
                text_blocks=document_1_text,
                extra={}
            ),
            document_2=DocumentContext(
                source=DocumentSource.from_monolingual_file(source.document_2),
                text_blocks=document_2_text,
                extra={}
            ),
        )
        result = StepResult(
            context=context,
        )

        logger.debug("monolingual single file pairs read successfully.")

        return result

    def _extract_single_file(self, source: MultilingualSingleFile) -> StepResult:
        raise NotImplementedError("Single file extraction is not implemented yet.")
