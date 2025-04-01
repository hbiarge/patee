import logging
from pathlib import Path
from typing import Union, Iterable, cast

import yaml

from patee import (MultilingualSingleFile, MonolingualSingleFilePair, NonPersistentStepsExecutor,
                   PersistentStepsExecutor, StepsBuilder, DefaultStepsBuilder, StepMetadata)
from patee.steps import ParallelExtractStep, ParallelProcessStep, StepResult

logger = logging.getLogger(__name__)

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

    @classmethod
    def load_from(cls, config_path: Path, steps_builder: StepsBuilder = None) -> "Patee":
        """Load the pipeline from a configuration file."""
        # Validate the config file exists
        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file {config_path} does not exist.")

        logger.debug(f"reading configuration file from {config_path}...")
        config = yaml.safe_load(config_path.read_text(encoding="utf-8"))

        if not steps_builder:
            logger.debug(f"no steps builder provided. Using default steps builder.")
            steps_builder = DefaultStepsBuilder()
        else:
            logger.debug(f"using provided steps builder: {steps_builder.__class__.__name__}")
            steps_builder = steps_builder

        instance = cls(steps_builder)

        step_idx = 0
        unique_step_names = set()
        for step in config["steps"]:
            step_type = step.get("type")
            if not step_type:
                raise ValueError("Step type is required in the configuration file.")

            logger.debug(f"loading step {step_type} at position {step_idx}...")

            step_name = step.get("name")
            if not step_name:
                step_name = step_type

            if step_name in unique_step_names:
                raise ValueError(f"Step names must be unique. Duplicate name found: {step_name}")

            step_config = step.get("config")
            if not step_config:
                step_config = {}

            metadata = StepMetadata(type=step_type, name=step_name, idx=step_idx, config=step_config)
            step_instance = instance._steps_builder.build(step_type, step_name, **step_config)

            instance._steps.append((step_instance, metadata))
            unique_step_names.add(step_name)

            logger.debug(f"step {step_type} with name {step_name} loaded successfully.")

            step_idx += 1

        logger.info(f"pipeline created successfully. Found {len(unique_step_names)} step(s).")

        return instance

    def remove_step(self, step_name: str) -> None:
        """Remove a step from the pipeline by name."""
        self._steps = [(step, metadata) for step, metadata in self._steps if step.name != step_name]

    def process(self, source: Union[MonolingualSingleFilePair, MultilingualSingleFile], out_dir: Union[Path, None] = None) -> StepResult:
        """Process source through the complete pipeline."""

        # Validate state of the pipeline is correct to start processing the source
        self._validate_steps_for_process()

        source_hash = hash(source)

        logger.debug(f"start processing source with hash {source_hash}...")

        if out_dir is None:
            logger.debug(f"no output directory provided. creating a NonPersistentStepsExecutor steps executor.")
            executor = NonPersistentStepsExecutor()
        else:
            # Validate the directory exists
            if not out_dir.exists():
                raise FileNotFoundError(f"Output directory {out_dir} does not exist.")

            logger.debug(f" output directory provided: {out_dir}. creating a PersistentStepsExecutor steps executor.")
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





