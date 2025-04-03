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
            raise ValueError("step must be a subclass of either ParallelExtractStep or ParallelProcessStep")

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
            raise ValueError("step must be a subclass of either ParallelExtractStep or ParallelProcessStep")

        if not result.should_stop_pipeline:
            result.context.dump_to(step_dir)

        logger.info("%s step executed in %s seconds.", step.name, 0)

        return result


class IntelligentPersistenceStepsExecutor(StepsExecutor):
    def __init__(self, source_hash: str, base_dir:Path):
        self.source_has_been_previously_executed = False

        self._source_hash = source_hash
        self._base_dir: Path = base_dir

        main_marker_file = self._base_dir / ".patee"
        self._validate_directory(base_dir, main_marker_file, source_hash)

    def execute_step(self, step: Union[ParallelExtractStep, ParallelProcessStep], metadata: StepMetadata,
                     source: Union[MonolingualSingleFilePair, MultilingualSingleFile, DocumentPairContext]) -> StepResult:
        step_dir = self._base_dir / step.name
        step_dir.mkdir(parents=True, exist_ok=True)
        step_marker_file = step_dir / ".patee"
        source_step_hash = f"{self._source_hash}--{hash(metadata)}"

        has_been_executed, open_mode = self._has_been_executed(step_marker_file, source_step_hash)

        if has_been_executed:
            logger.info(
                "the step %s with hash %s have already been executed in %s. Skipping...",
                step.name, source_step_hash, step_dir)

            result = self._load_result_from_previous_execution(source, step_dir)

            return result
        else:
            logger.info("start executing %s step in persistent mode...", step.name)

            context = StepContext(step_dir=step_dir)

            if isinstance(step, ParallelExtractStep) and not isinstance(source, DocumentPairContext):
                result = step.extract(context, source)
            elif isinstance(step, ParallelProcessStep) and isinstance(source, DocumentPairContext):
                result = step.process(context, source)
            else:
                raise ValueError("step must be a subclass of either ParallelExtractStep or ParallelProcessStep")

            if not result.should_stop_pipeline:
                result.context.dump_to(step_dir)

            # Save the hash of the source step to the marker file
            with step_marker_file.open(open_mode, encoding="utf-8") as f:
                f.write(source_step_hash + "\n")

            logger.info("%s step executed in %s seconds", step.name, 0)

            return result

    @staticmethod
    def _has_been_executed(step_marker_file: Path, source_step_hash: str) -> (bool, str):
        if step_marker_file.exists():
            for existing_hash in step_marker_file.read_text(encoding="utf-8").splitlines():
                if existing_hash == source_step_hash:
                    return True, "a"
            return False, "a"
        else:
            return False, "w"

    @staticmethod
    def _get_document_sources(
            source: Union[MonolingualSingleFilePair, MultilingualSingleFile, DocumentPairContext],
    ) -> (DocumentSource, DocumentSource):
        if isinstance(source, MonolingualSingleFilePair):
            return (
                DocumentSource.from_monolingual_file(source.document_1),
                DocumentSource.from_monolingual_file(source.document_2),
            )
        elif isinstance(source, MultilingualSingleFile):
            return (
                DocumentSource.from_multilingual_file(source, 0),
                DocumentSource.from_multilingual_file(source, 1),
            )
        elif isinstance(source, DocumentPairContext):
            return source.document_1.source, source.document_2.source
        else:
            raise ValueError("Unknown source type")

    def _validate_directory(self, base_dir: Path, main_marker_file: Path, source_hash: str):
        has_been_executed, open_mode = self._has_been_executed(main_marker_file, source_hash)
        if has_been_executed:
            logger.info("the source with hash %s has been executed before in %s", source_hash, base_dir)
            self.source_has_been_previously_executed = True
        else:
            logger.info("the source with hash %s has not been executed before in %s", source_hash, base_dir)
            self.source_has_been_previously_executed = False

        with main_marker_file.open(open_mode, encoding="utf-8") as f:
            f.write(source_hash + "\n")


    def _load_result_from_previous_execution(self, source, step_dir: Path) -> StepResult:
        document_1_source, document_2_source = self._get_document_sources(source)
        document_1_saved_result = step_dir / f"{document_1_source.document_path.stem}.txt"
        document_2_saved_result = step_dir / f"{document_1_source.document_path.stem}.txt"

        logger.debug("reading document 1 from %s ...", document_1_saved_result)
        document_1_text = document_1_saved_result.read_text(encoding="utf-8")

        logger.debug("reading document 2 from %s ...", document_2_saved_result)
        document_2_text = document_2_saved_result.read_text(encoding="utf-8")

        context = DocumentPairContext(
            document_1=DocumentContext(
                source=document_1_source,
                text=document_1_text,
                extra={}
            ),
            document_2=DocumentContext(
                source=document_2_source,
                text=document_2_text,
                extra={}
            ),
        )
        result = StepResult(
            context=context,
            skipped=True,
        )
        return result
