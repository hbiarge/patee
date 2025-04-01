from abc import ABC, abstractmethod

from patee.steps import Step, DoclingExtractor, NoopProcessorStep, TextReaderExtractor


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
            "text_reader_extractor",
            "docling_extractor",
            "noop_step",
        }

    def get_supported_step_types(self) -> set[str]:
        return self._supported_steps

    def build(self, step_type:str, step_name:str, **kwargs) -> Step:
        if step_type == "text_reader_extractor":
            return TextReaderExtractor(step_name, **kwargs)
        elif step_type == "docling_extractor":
            return DoclingExtractor(step_name, **kwargs)
        elif step_type == "noop_step":
            return NoopProcessorStep(step_name, **kwargs)
        else:
            raise ValueError(f"Unsupported step: {step_type}")
