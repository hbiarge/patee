import logging
from abc import abstractmethod
from pathlib import Path
from typing import Union

from patee import MonolingualSingleFilePair, MultilingualSingleFile, StepMetadata
from patee.steps import ParallelExtractStep, StepResult, ParallelProcessStep

logger = logging.getLogger(__name__)

class StepsExecutor:

    @abstractmethod
    def execute_extract_step(self, extract_step: ParallelExtractStep, metadata: StepMetadata,
                             source: Union[MonolingualSingleFilePair, MultilingualSingleFile]) -> StepResult:
        pass

    @abstractmethod
    def execute_process_step(self, process_step: ParallelProcessStep, metadata: StepMetadata,
                             source: StepResult) -> StepResult:
        pass


class NonPersistentStepsExecutor(StepsExecutor):

    def execute_extract_step(self, extract_step: ParallelExtractStep, metadata: StepMetadata,
                             source: Union[MonolingualSingleFilePair, MultilingualSingleFile]) -> StepResult:
        logger.info(f"start executing {extract_step.name} step...")
        result = extract_step.extract(source=source)
        logger.info(f"{extract_step.name} step executed in NotImplemented seconds")
        return result

    def execute_process_step(self, process_step: ParallelProcessStep, metadata: StepMetadata,
                             source: StepResult) -> StepResult:
        logger.info(f"start executing {process_step.name} step...")
        result = process_step.process(source=source)
        logger.info(f"{process_step.name} step executed in NotImplemented seconds")
        return result

class PersistentStepsExecutor(StepsExecutor):
    def __init__(self, base_dir: Path):
        self._base_dir: Path = base_dir

    def execute_extract_step(self, extract_step: ParallelExtractStep, metadata: StepMetadata,
                             source: Union[MonolingualSingleFilePair, MultilingualSingleFile]) -> StepResult:
        step_dir = self._base_dir / extract_step.name
        step_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"start executing {extract_step.name} step...")

        result = extract_step.extract(source=source)
        result.write_to_files(step_dir)

        logger.info(f"{extract_step.name} step executed in NotImplemented seconds")

        return result

    def execute_process_step(self, process_step: ParallelProcessStep, metadata: StepMetadata,
                             source: StepResult) -> StepResult:
        step_dir = self._base_dir / process_step.name
        step_dir.mkdir(parents=True, exist_ok=True)

        logger.info(f"start executing {process_step.name} step...")

        result = process_step.process(source)
        result.write_to_files(step_dir)

        logger.info(f"{process_step.name} step executed in NotImplemented seconds")

        return result


class IntelligentPersistenceStepsExecutor(StepsExecutor):
    """Class to execute the steps in the pipeline."""
    def __init__(self, source_hash, base_dir:Path):
        self._source_hash = source_hash
        self._base_dir: Path = base_dir

        main_marker_file = self._base_dir / ".patee"

        if main_marker_file.exists():
            # TODO: Analyze marker file to determine if the source has been processed before
            logger.info(f"the source with hash {source_hash} has been executed before in {base_dir}")
            self.source_has_been_previously_executed = True
        else:
            logger.info(f"the source with hash {source_hash} has not been executed before in {base_dir}")
            self.source_has_been_previously_executed = False
            # TODO: Create the main marker file


    def execute_extract_step(self, extract_step: ParallelExtractStep, metadata: StepMetadata,
                             source: Union[MonolingualSingleFilePair, MultilingualSingleFile]) -> StepResult:
        step_dir = self._base_dir / extract_step.name
        step_dir.mkdir(parents=True, exist_ok=True)
        step_marker_file = step_dir / ".patee"

        if self.source_has_been_previously_executed and step_marker_file.exists():
            logger.info(f"the step {extract_step.name} for source hash {self._source_hash} have already been executed in {step_dir}. Skipping...")

            # TODO: Analyze marker file to determine if the source has been processed before
            # TODO: Create the result reading the files from the step_dir
            return extract_step.extract(source=source)
        else:
            logger.info(f"start executing {extract_step.name} step...")

            result = extract_step.extract(source=source)
            result.write_to_files(step_dir)
            # TODO: Create the step marker file

            logger.info(f"{extract_step.name} step executed in NotImplemented seconds")

            return result

    def execute_process_step(self, process_step: ParallelProcessStep, metadata: StepMetadata, source: StepResult) -> StepResult:
        step_dir = self._base_dir / process_step.name
        step_marker_file = step_dir / ".patee"

        step_dir.mkdir(parents=True, exist_ok=True)

        if self.source_has_been_previously_executed and step_marker_file.exists():
            logger.info(
                f"the step {process_step.name} for source hash {self._source_hash} have already been executed in {step_dir}. Skipping...")
            # TODO: Analyze marker file to determine if the source has been processed before
            # TODO: Create the result reading the files from the step_dir
            return process_step.process(source)
        else:
            logger.info(f"start executing {process_step.name} step...")

            result = process_step.process(source)
            result.write_to_files(step_dir)
            # TODO: Create the step marker file

            logger.info(f"{process_step.name} step executed in NotImplemented seconds")

            return result
