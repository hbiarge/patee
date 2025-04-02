import pytest

from patee import DefaultStepsBuilder
from patee.steps import TextReaderExtractor, DoclingExtractor, NoopProcessorStep


class TestDefaultStepsBuilder:
    def setup_method(self):
        self.builder = DefaultStepsBuilder()

    def test_get_supported_step_types(self):
        expected_types: set[str] = {
            "text_reader_extractor",
            "docling_extractor",
            "noop_step_processor",
            "human_in_the_loop_processor",
        }
        assert self.builder.get_supported_step_types() == expected_types

    def test_build_text_reader_extractor(self):
        # Test creating a TextReaderExtractor step
        step = self.builder.build("text_reader_extractor", "text_reader")

        assert isinstance(step, TextReaderExtractor)
        assert step.name == "text_reader"

    def test_build_docling_extractor(self):
        # Test creating a DoclingExtractor step
        step = self.builder.build("docling_extractor", "docling_step")

        assert isinstance(step, DoclingExtractor)
        assert step.name == "docling_step"

    def test_build_noop_step(self):
        # Test creating a NoopProcessorStep
        step = self.builder.build("noop_step_processor", "noop")

        assert isinstance(step, NoopProcessorStep)
        assert step.name == "noop"

    def test_build_unsupported_step(self):
        # Test raising error for unsupported step type
        with pytest.raises(ValueError, match=r"Unsupported step: unknown_step"):
            self.builder.build("unknown_step", "test_step")
