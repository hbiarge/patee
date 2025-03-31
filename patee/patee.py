import logging
from pathlib import Path
from typing import Union, Iterable, cast

import yaml

from patee import (MultilingualSingleFile, MonolingualSingleFilePair, NonPersistentStepsExecutor,
                   PersistentStepsExecutor, StepsBuilder, DefaultStepsBuilder, StepMetadata)
from patee.steps import ParallelExtractStep, ParallelProcessStep, StepResult


class Patee:
    """Main pipeline class to coordinate the processing steps."""

    def __init__(self, steps_builder: StepsBuilder = None):
        """Initialize the pipeline with processing steps."""
        if steps_builder is None:
            self._steps_builder = DefaultStepsBuilder()
        else:
            self._steps_builder = steps_builder
        self._steps = []

    @property
    def step_names(self) -> Iterable[str]:
        """Return the name of the steps."""
        return [step.name for step, _ in self._steps]

    @property
    def is_valid(self) -> bool:
        if not self._steps:
            return False

        first_step, _ = self._steps[0]
        if not isinstance(first_step, ParallelExtractStep):
            return False

        for step, _ in self._steps[1:]:
            if not isinstance(step, ParallelProcessStep):
                return False

        return True

    @classmethod
    def load_from(cls, steps_config_path: Path, steps_builder: StepsBuilder = None) -> "Patee":
        """Load the pipeline from a configuration file."""
        # Validate the config file exists
        if not steps_config_path.exists():
            raise FileNotFoundError(f"Configuration file {steps_config_path} does not exist.")

        config = yaml.safe_load(steps_config_path.read_text(encoding="utf-8"))
        steps_builder = steps_builder or DefaultStepsBuilder()

        instance = cls(steps_builder)

        step_idx = 0
        for step in config["steps"]:
            step_type = step.get("type")
            if not step_type:
                raise ValueError("Step type is required in the configuration file.")

            step_name = step.get("name")
            if not step_name:
                step_name = step_type

            step_config = step.get("config")
            if not step_config:
                step_config = {}

            metadata = StepMetadata(type=step_type, name=step_name, idx=step_idx, config=step_config)
            step_instance = instance._steps_builder.build(step_type, step_name, **step_config)

            instance._steps.append((step_instance, metadata))
            step_idx += 1

        unique_step_names = set(step.name for step, _ in instance._steps)
        if len(unique_step_names) != len(instance._steps):
            raise ValueError("Step names must be unique in the pipeline configuration.")

        logging.debug(f"Pipeline created successfully. Found {len(unique_step_names)} unique step names.")

        return instance

    def remove_step(self, step_name: str) -> None:
        """Remove a step from the pipeline by name."""
        self._steps = [(step, metadata) for step, metadata in self._steps if step.name != step_name]

    def process(self, source: Union[MonolingualSingleFilePair, MultilingualSingleFile], out_dir: Union[Path, None] = None) -> StepResult:
        """Process source through the complete pipeline."""

        # Validate state of the pipeline is correct to start processing the source
        self._validate_steps_for_process()

        if out_dir is None:
            executor = NonPersistentStepsExecutor()
        else:
            # Validate the directory exists
            if not out_dir.exists():
                raise FileNotFoundError(f"Output directory {out_dir} does not exist.")

            executor = PersistentStepsExecutor(base_dir=out_dir)

        extract_step, extract_metadata = self._steps[0]
        extract_result = executor.execute_extract_step(cast(ParallelExtractStep ,extract_step), extract_metadata, source)

        step_result = extract_result
        for step, metadata in self._steps[1:]:
            step_result = executor.execute_process_step(cast(ParallelProcessStep ,step), metadata, step_result)

        return step_result

    def _validate_steps_for_process(self):
        """Validate the steps in the pipeline."""
        if not self._steps:
            raise ValueError("No processing steps defined in the pipeline.")

        # Get the first step and validate is an instance of ExtractStep class
        first_step, _ = self._steps[0]
        if not isinstance(first_step, ParallelExtractStep):
            raise ValueError(f"First step must be an instance of ExtractStep, got {type(first_step)} instead.")

        # Validate that all other steps are instances of ParallelTextStep
        for step, _ in self._steps[1:]:
            if not isinstance(step, ParallelProcessStep):
                raise ValueError(f"All steps must be instances of ParallelTextStep, got {type(step)} instead.")





