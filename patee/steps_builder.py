from abc import ABC, abstractmethod

from patee.steps import Step, DoclingExtractor, NoopProcessorStep
from patee.steps.text_reader_extractor_step import TextReaderExtractor


class StepsBuilder(ABC):
    """Abstract class for building processing steps."""

    # TODO: Should expose a method to get the list of supported steps

    @abstractmethod
    def build(self, step_type: str, step_name: str, **kwargs) -> Step:
        pass


class DefaultStepsBuilder(StepsBuilder):
    def build(self, step_type:str, step_name:str, **kwargs) -> Step:
        if step_type == "text_reader_extractor":
            return TextReaderExtractor(step_name, **kwargs)
        elif step_type == "docling_extractor":
            return DoclingExtractor(step_name, **kwargs)
        elif step_type == "noop_step":
            return NoopProcessorStep(step_name, **kwargs)
        else:
            raise ValueError(f"Unsupported step: {step_type}")
