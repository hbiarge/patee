import logging
from abc import abstractmethod, ABC
from pathlib import Path
from typing import Union

from patee import MonolingualSingleFilePair, MultilingualSingleFile, StepMetadata
from patee.steps import (
    StepContext,
    ParallelExtractStep,
    StepResult,
    ParallelProcessStep,
    DocumentContext,
    DocumentSource,
    DocumentPairContext,
)

logger = logging.getLogger(__name__)


class StepsExecutor(ABC):
    @abstractmethod
    def execute_step(self, step: Union[ParallelExtractStep, ParallelProcessStep], metadata: StepMetadata,
                     source: Union[MonolingualSingleFilePair, MultilingualSingleFile, DocumentPairContext]) -> StepResult:
        pass


class NonPersistentStepsExecutor(StepsExecutor):
    def execute_step(self, step: Union[ParallelExtractStep, ParallelProcessStep], metadata: StepMetadata,
                     source: Union[MonolingualSingleFilePair, MultilingualSingleFile, DocumentPairContext]) -> StepResult:
        logger.info("start executing %s step in non persistent mode...", step.name)

        context = StepContext(step_dir=None)

        if isinstance(step, ParallelExtractStep) and not isinstance(source, DocumentPairContext):
            result = step.extract(context, source)
        elif isinstance(step, ParallelProcessStep) and isinstance(source, DocumentPairContext):
            result = step.process(context, source)
        else:
            raise ValueError("Unknown step type")

        logger.info("%s step executed in %s seconds.", step.name, 0)
        return result


class PersistentStepsExecutor(StepsExecutor):
    def __init__(self, base_dir: Path):
        self._base_dir: Path = base_dir

    def execute_step(self, step: Union[ParallelExtractStep, ParallelProcessStep], metadata: StepMetadata,
                     source: Union[MonolingualSingleFilePair, MultilingualSingleFile, DocumentPairContext]) -> StepResult:
        step_dir = self._base_dir / step.name
        step_dir.mkdir(parents=True, exist_ok=True)

        logger.info("start executing %s step in persistent mode...", step.name)

        context = StepContext(step_dir=step_dir)

        if isinstance(step, ParallelExtractStep) and not isinstance(source, DocumentPairContext):
            result = step.extract(context, source)
        elif isinstance(step, ParallelProcessStep) and isinstance(source, DocumentPairContext):
            result = step.process(context, source)
        else:
            raise ValueError("Unknown step type")

        result.context.dump_to(step_dir)

        logger.info("%s step executed in %s seconds.", step.name, 0)

        return result


class IntelligentPersistenceStepsExecutor(StepsExecutor):
    """Class to execute the steps in the pipeline."""
    def __init__(self, source_hash, base_dir:Path):
        self._source_hash = source_hash
        self._base_dir: Path = base_dir

        main_marker_file = self._base_dir / ".patee"

        if main_marker_file.exists():
            # TODO: Analyze marker file to determine if the source has been processed before
            logger.info("the source with hash %s has been executed before in %s", source_hash, base_dir)
            self.source_has_been_previously_executed = True
        else:
            logger.info("the source with hash %s has not been executed before in %s", source_hash, base_dir)
            self.source_has_been_previously_executed = False
            # TODO: Create the main marker file

    def execute_step(self, step: Union[ParallelExtractStep, ParallelProcessStep], metadata: StepMetadata,
                     source: Union[MonolingualSingleFilePair, MultilingualSingleFile, DocumentPairContext]) -> StepResult:
        step_dir = self._base_dir / step.name
        step_dir.mkdir(parents=True, exist_ok=True)
        step_marker_file = step_dir / ".patee"
        step_metadata_hash = hash(metadata)

        if self.source_has_been_previously_executed and step_marker_file.exists():
            logger.info(
                "the step %s with metadata hash %s and source hash %s have already been executed in %s. Skipping...",
                step.name, step_metadata_hash, self._source_hash, step_dir)

            # TODO: Analyze marker file to determine if the source has been processed before
            # TODO: Create the result reading the files from the step_dir
            document_1_saved_result,document_2_saved_result = self._get_results_file_paths(step_dir, source)

            logger.debug("reading document 1 from %s ...", document_1_saved_result)
            document_1_text = document_1_saved_result.read_text(encoding="utf-8")

            logger.debug("reading document 2 from %s ...", document_2_saved_result)
            document_2_text = document_2_saved_result.read_text(encoding="utf-8")

            context = DocumentPairContext(
                document_1=DocumentContext(
                    source=DocumentSource.from_monolingual_file(source.document_1),
                    text=document_1_text,
                    extra={}
                ),
                document_2=DocumentContext(
                    source=DocumentSource.from_monolingual_file(source.document_2),
                    text=document_2_text,
                    extra={}
                ),
            )
            result = StepResult(
                context=context,
            )

            return result
        else:
            logger.info("start executing %s step in persistent mode...", step.name)

            context = StepContext(step_dir=step_dir)

            if isinstance(step, ParallelExtractStep) and not isinstance(source, DocumentPairContext):
                result = step.extract(context, source)
            elif isinstance(step, ParallelProcessStep) and isinstance(source, DocumentPairContext):
                result = step.process(context, source)
            else:
                raise ValueError("Unknown step type")

            result.context.dump_to(step_dir)
            # TODO: Create the step marker file

            logger.info("%s step executed in %s seconds", step.name, 0)

            return result

    def _get_results_file_paths(self, step_dir: Path,
                                source: Union[MonolingualSingleFilePair, MultilingualSingleFile, DocumentPairContext]):
        if isinstance(source, MonolingualSingleFilePair):
            document_1_saved_result = step_dir / f"{source.document_1.document_path.stem}.txt"
            document_2_saved_result = step_dir / f"{source.document_2.document_path.stem}.txt"
        elif isinstance(source, MultilingualSingleFile):
            document_1_saved_result = step_dir / f"{source.document_path.stem}-{source.iso2_languages[0]}.txt"
            document_2_saved_result = step_dir / f"{source.document_path.stem}-{source.iso2_languages[1]}.txt"
        elif isinstance(source, DocumentPairContext):
            document_1_saved_result = step_dir / f"{source.document_1.source.document_path.stem}.txt"
            document_2_saved_result = step_dir / f"{source.document_2.source.document_path.stem}.txt"
        else:
            raise ValueError("Unknown source type")

        return document_1_saved_result, document_2_saved_result
