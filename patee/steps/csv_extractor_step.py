import logging
from typing import Union

import pandas as pd

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


class CsvExtractor(ParallelExtractStep):
    def __init__(self, name: str, pipeline_context: PipelineContext, **kwargs):
        super().__init__(name, pipeline_context)

    @staticmethod
    def step_type() -> str:
        return "csv_extractor"

    def extract(self, context: StepContext,
                source: Union[MonolingualSingleFilePair, MultilingualSingleFile]) -> StepResult:
        if isinstance(source, MonolingualSingleFilePair):
            return self._extract_file_pair(source)
        elif isinstance(source, MultilingualSingleFile):
            return self._extract_single_file(source)
        else:
            raise ValueError(f"Unsupported source type: {type(source)}")

    def _extract_file_pair(self, source: MonolingualSingleFilePair) -> StepResult:
        raise NotImplementedError("Multi file extraction is not implemented yet.")

    def _extract_single_file(self, source: MultilingualSingleFile) -> StepResult:
        df = pd.read_csv(source.document_path)

        language_1_blocks = []
        language_2_blocks = []

        for index, row in df.iterrows():
            language_1_blocks.append(row[0])
            language_2_blocks.append(row[1])

        context = DocumentPairContext(
            document_1=DocumentContext(
                source=DocumentSource.from_multilingual_file(source, 0),
                text_blocks=language_1_blocks,
                extra={}
            ),
            document_2=DocumentContext(
                source=DocumentSource.from_multilingual_file(source, 1),
                text_blocks=language_2_blocks,
                extra={}
            ),
        )
        result = StepResult(
            context=context,
        )

        logger.debug("monolingual single file pairs read successfully.")

        return result