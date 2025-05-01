import logging
from dataclasses import dataclass
from typing import Union, cast

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


@dataclass(frozen=True)
class MultilingualFileCsvConfig:
    language_1_idx: int
    language_2_idx: int
    options: dict = None


@dataclass(frozen=True)
class MonolingualFileCsvConfig:
    language_idx: int
    options: dict = None


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
        if source.shared_config is not None:
            if not isinstance(source.shared_config, MultilingualFileCsvConfig):
                raise ValueError("shared config must be of type MultilingualFileCsvConfig")

            shared_config = cast(MultilingualFileCsvConfig, source.shared_config)
            df1 = pd.read_csv(source.document_1.document_path, **shared_config.options)
            language_1_blocks = df1[shared_config.language_1_idx].tolist()
            df2 = pd.read_csv(source.document_2.document_path, **shared_config.options)
            language_2_blocks = df2[shared_config.language_2_idx].tolist()
        else:
            if not isinstance(source.document_1.config, MonolingualFileCsvConfig):
                raise ValueError("individual config must be of type MonolingualFileCsvConfig")
            if not isinstance(source.document_2.config, MonolingualFileCsvConfig):
                raise ValueError("individual config must be of type MonolingualFileCsvConfig")

            config_1 = cast(MonolingualFileCsvConfig, source.document_1.config)
            config_2 = cast(MonolingualFileCsvConfig, source.document_2.config)
            df1 = pd.read_csv(source.document_1.document_path, **config_1.options)
            language_1_blocks = df1[config_1.language_idx].tolist()
            df2 = pd.read_csv(source.document_2.document_path, **config_2.options)
            language_2_blocks = df2[config_2.language_idx].tolist()

        context = DocumentPairContext(
            document_1=DocumentContext(
                source=DocumentSource.from_monolingual_file(source.document_1),
                text_blocks=language_1_blocks,
                extra={}
            ),
            document_2=DocumentContext(
                source=DocumentSource.from_monolingual_file(source.document_2),
                text_blocks=language_2_blocks,
                extra={}
            ),
        )
        result = StepResult(
            context=context,
        )

        return result

        if source.document_1.config is not None and not isinstance(source.document_1.config, MonolingualFileCsvConfig):
            raise ValueError("individual config must be of type MonolingualFileCsvConfig")

    def _extract_single_file(self, source: MultilingualSingleFile) -> StepResult:
        if source.config is not None and not isinstance(source.config, MultilingualFileCsvConfig):
            raise ValueError("Invalid config type for MultilingualSingleFile")

        config = cast(MultilingualFileCsvConfig, source.config)

        df = pd.read_csv(source.document_path, **config.options)

        language_1_blocks = df[config.language_1_idx].tolist()
        language_2_blocks = df[config.language_2_idx].tolist()

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