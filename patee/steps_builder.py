from abc import ABC, abstractmethod

from .step_types import Step
from .steps.text_reader_extractor_step import TextReaderExtractor
from .steps.docling_extractor_step import DoclingExtractor
from .steps.human_in_the_loop_processor_step import HumanInTheLoopProcessorStep
from .steps.noop_processor_step import NoopProcessorStep


class StepsBuilder(ABC):
    """Abstract class for building processing steps."""

    @abstractmethod
    def get_supported_step_types(self) -> set[str]:
        """Get the supported step types."""
        pass

    @abstractmethod
    def build(self, step_type: str, step_name: str, **kwargs) -> Step:
        pass


class DefaultStepsBuilder(StepsBuilder):
    def __init__(self):
        super().__init__()
        self._supported_steps: set[str] = {
            TextReaderExtractor.step_type(),
            DoclingExtractor.step_type(),
            NoopProcessorStep.step_type(),
            HumanInTheLoopProcessorStep.step_type()
        }

    def get_supported_step_types(self) -> set[str]:
        return self._supported_steps

    def build(self, step_type:str, step_name:str, **kwargs) -> Step:
        if step_type == TextReaderExtractor.step_type():
            return TextReaderExtractor(step_name, **kwargs)
        elif step_type == DoclingExtractor.step_type():
            return DoclingExtractor(step_name, **kwargs)
        elif step_type == NoopProcessorStep.step_type():
            return NoopProcessorStep(step_name, **kwargs)
        elif step_type == HumanInTheLoopProcessorStep.step_type():
            return HumanInTheLoopProcessorStep(step_name, **kwargs)
        else:
            raise ValueError(f"Unsupported step: {step_type}")
