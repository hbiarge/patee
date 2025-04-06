import pytest

from patee.steps_builder.default_steps_builder import DefaultStepsBuilder
from patee.steps.text_reader_extractor_step import TextReaderExtractor
from patee.steps.docling_extractor_step import DoclingExtractor
from patee.steps.noop_processor_step import NoopProcessorStep
from tests.utils.mothers.contexts import get_pipeline_context


class TestDefaultStepsBuilder:
    def setup_method(self):
        self.builder = DefaultStepsBuilder()

    def test_get_supported_step_types(self):
        expected_types: set[str] = {
            "text_reader_extractor",
            "docling_extractor",
            "noop",
            "human_in_the_loop",
            "write_to_file",
        }
        assert self.builder.get_supported_step_types() == expected_types

    def test_build_text_reader_extractor(self):
        context = get_pipeline_context()
        step = self.builder.build("text_reader_extractor", "text_reader", context)

        assert isinstance(step, TextReaderExtractor)
        assert step.name == "text_reader"

    def test_build_docling_extractor(self):
        context = get_pipeline_context()
        step = self.builder.build("docling_extractor", "docling_step", context)

        assert isinstance(step, DoclingExtractor)
        assert step.name == "docling_step"

    def test_build_noop_step(self):
        context = get_pipeline_context()
        step = self.builder.build("noop", "noop", context)

        assert isinstance(step, NoopProcessorStep)
        assert step.name == "noop"

    def test_build_unsupported_step(self):
        context = get_pipeline_context()
        with pytest.raises(ValueError, match=r"Unsupported step: unknown_step"):
            self.builder.build("unknown_step", "test_step", context)
