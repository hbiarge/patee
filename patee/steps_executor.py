import logging
from abc import abstractmethod
from pathlib import Path
from typing import Union

from patee import MonolingualSingleFilePair, MultilingualSingleFile, StepMetadata
from patee.steps import ParallelExtractStep, StepResult, ParallelProcessStep


class StepsExecutor:
    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def execute_extract_step(self, extract_step: ParallelExtractStep, metadata: StepMetadata,
                             source: Union[MonolingualSingleFilePair, MultilingualSingleFile]) -> StepResult:
        pass

    @abstractmethod
    def execute_process_step(self, process_step: ParallelProcessStep, metadata: StepMetadata,
                             source: StepResult) -> StepResult:
        pass


class NonPersistentStepsExecutor(StepsExecutor):
    def start(self):
        pass

    def execute_extract_step(self, extract_step: ParallelExtractStep, metadata: StepMetadata,
                             source: Union[MonolingualSingleFilePair, MultilingualSingleFile]) -> StepResult:
        return extract_step.extract(source=source)

    def execute_process_step(self, process_step: ParallelProcessStep, metadata: StepMetadata,
                             source: StepResult) -> StepResult:
        return process_step.process(source=source)

class PersistentStepsExecutor(StepsExecutor):
    def __init__(self, base_dir: Path):
        self._base_dir: Path = base_dir

    def start(self):
        pass

    def execute_extract_step(self, extract_step: ParallelExtractStep, metadata: StepMetadata,
                             source: Union[MonolingualSingleFilePair, MultilingualSingleFile]) -> StepResult:
        step_dir = self._base_dir / extract_step.name
        step_dir.mkdir(parents=True, exist_ok=True)

        result = extract_step.extract(source=source)
        result.write_to_files(step_dir)

        return result

    def execute_process_step(self, process_step: ParallelProcessStep, metadata: StepMetadata,
                             source: StepResult) -> StepResult:
        step_dir = self._base_dir / process_step.name
        step_dir.mkdir(parents=True, exist_ok=True)

        result = process_step.process(source)
        result.write_to_files(step_dir)

        return result


class IntelligentPersistenceStepsExecutor(StepsExecutor):
    """Class to execute the steps in the pipeline."""
    def __init__(self, source: Union[MonolingualSingleFilePair, MultilingualSingleFile], base_dir:Path):
        self._source_hash = hash(source)
        self._base_dir: Path = base_dir

        self.source_has_been_previously_executed = False

    def start(self):
        main_marker_file = self._base_dir / ".patee"

        logging.info(f"Starting pipeline execution for source hash {self._source_hash}")

        if main_marker_file.exists():
            # TODO: Analyze marker file to determine if the source has been processed before
            self.source_has_been_previously_executed = True
        else:
            self.source_has_been_previously_executed = False
            # TODO: Create the main marker file

    def execute_extract_step(self, extract_step: ParallelExtractStep, metadata: StepMetadata,
                             source: Union[MonolingualSingleFilePair, MultilingualSingleFile]) -> StepResult:
        step_dir = self._base_dir / extract_step.name
        step_marker_file = step_dir / ".patee"

        step_dir.mkdir(parents=True, exist_ok=True)

        if step_marker_file.exists():
            # TODO: Analyze marker file to determine if the source has been processed before
            # TODO: Create the result reading the files from the step_dir
            return extract_step.extract(source=source)
        else:
            result = extract_step.extract(source=source)
            result.write_to_files(step_dir)
            # TODO: Create the step marker file
            return result

    def execute_process_step(self, process_step: ParallelProcessStep, metadata: StepMetadata, source: StepResult) -> StepResult:
        step_dir = self._base_dir / process_step.name
        step_marker_file = step_dir / ".patee"

        step_dir.mkdir(parents=True, exist_ok=True)

        if step_marker_file.exists():
            # TODO: Analyze marker file to determine if the source has been processed before
            # TODO: Create the result reading the files from the step_dir
            return process_step.process(source)
        else:
            result = process_step.process(source)
            result.write_to_files(step_dir)
            # TODO: Create the step marker file
            return result
