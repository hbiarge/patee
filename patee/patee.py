from dataclasses import dataclass
from pathlib import Path
from typing import Union, Iterable, cast

import yaml

from patee.input_types import  MultilingualSingleFile, MonolingualSingleFilePair
from patee.steps import ParallelExtractStep, StepResult

from patee.steps.core_step_types import ParallelProcessStep
from patee.steps_builder import StepsBuilder, DefaultStepsBuilder


class Patee:
    """Main pipeline class to coordinate the processing steps."""

    def __init__(self, step_builder: StepsBuilder =None):
        """Initialize the pipeline with processing steps."""
        if step_builder is None:
            self._step_builder = DefaultStepsBuilder()
        else:
            self._step_builder = step_builder
        self._steps = []
        self._executed_steps = []

    @property
    def step_names(self) -> Iterable[str]:
        """Return the name of the steps."""
        return [step.name for step in self._steps]

    @property
    def is_valid(self) -> bool:
        if not self._steps:
            return False

        first_step = self._steps[0]
        if not isinstance(first_step, ParallelExtractStep):
            return False

        for step in self._steps[1:]:
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

            step_instance = instance._step_builder.build(step_type, step_name, **step_config)

            instance._steps.append(step_instance)

        unique_step_names = set(step.name for step in instance._steps)
        if len(unique_step_names) != len(instance._steps):
            raise ValueError("Step names must be unique in the pipeline configuration.")

        return instance

    def remove_step(self, step_name: str) -> None:
        """Remove a step from the pipeline by name."""
        self._steps = [step for step in self._steps if step.name != step_name]

    def process(
            self,
            source: Union[MonolingualSingleFilePair, MultilingualSingleFile],
            out_dir: Path
    ) -> StepResult:
        """Process PDF through the complete pipeline."""

        self._validate_steps_for_process()

        # Ensure the output directory exists
        out_dir.mkdir(parents=True, exist_ok=True)

        extract_step = cast(ParallelExtractStep ,self._steps[0])
        extract_step_dir = out_dir / extract_step.name
        extract_result = extract_step.extract(source)
        self._write_result(extract_result, extract_step_dir)

        step_result = extract_result
        for step in self._steps[1:]:
            step_dir = out_dir / step.name
            step_result = step.process(step_result)
            self._write_result(step_result, step_dir)

            self._executed_steps.append(step.name)

        return step_result

    def _validate_steps_for_process(self):
        """Validate the steps in the pipeline."""
        if not self._steps:
            raise ValueError("No processing steps defined in the pipeline.")

        # Get the first step and validate is an instance of ExtractStep class
        first_step = self._steps[0]
        if not isinstance(first_step, ParallelExtractStep):
            raise ValueError(f"First step must be an instance of ExtractStep, got {type(first_step)} instead.")

        # Validate that all other steps are instances of ParallelTextStep
        for step in self._steps[1:]:
            if not isinstance(step, ParallelProcessStep):
                raise ValueError(f"All steps must be instances of ParallelTextStep, got {type(step)} instead.")

    @staticmethod
    def _write_result(result: StepResult, result_dir: Path):
        # Ensure the output directory exists
        result_dir.mkdir(parents=True, exist_ok=True)

        # Write the extracted text to a file
        result.write_to_files(result_dir)




