import pytest

from patee.steps.csv_extractor_step import CsvExtractor
from patee.steps.docling_extractor_step import DoclingExtractor
from patee.steps.human_in_the_loop_processor_step import HumanInTheLoopProcessorStep
from patee.steps.noop_processor_step import NoopProcessorStep
from patee.steps.text_extractor_step import TextReaderExtractor
from patee.steps.text_writer_processor_step import TextWriterProcessorStep
from patee.steps_builder.default_steps_builder import DefaultStepsBuilder
from tests.utils.mothers.contexts import get_pipeline_context


class TestDefaultStepsBuilder:
    def setup_method(self):
        self.builder = DefaultStepsBuilder()

    def test_get_supported_step_types(self):
        expected_types: set[str] = {
            "text_extractor",
            "docling_extractor",
            "csv_extractor",
            "noop",
            "human_in_the_loop",
            "write_to_file",
        }
        assert self.builder.get_supported_step_types() == expected_types

    def test_build_text_reader_extractor(self):
        context = get_pipeline_context()
        step = self.builder.build("text_extractor", "text_reader", context)

        assert isinstance(step, TextReaderExtractor)
        assert step.name == "text_reader"

    def test_build_docling_extractor(self):
        context = get_pipeline_context()
        step = self.builder.build("docling_extractor", "docling_step", context)

        assert isinstance(step, DoclingExtractor)
        assert step.name == "docling_step"

    def test_build_csv_extractor(self):
        context = get_pipeline_context()
        step = self.builder.build("csv_extractor", "csv_step", context)

        assert isinstance(step, CsvExtractor)
        assert step.name == "csv_step"

    def test_build_noop_step(self):
        context = get_pipeline_context()
        step = self.builder.build("noop", "noop", context)

        assert isinstance(step, NoopProcessorStep)
        assert step.name == "noop"

    def test_build_human_in_the_loop_step(self):
        context = get_pipeline_context()
        step = self.builder.build("human_in_the_loop", "hitl", context)

        assert isinstance(step, HumanInTheLoopProcessorStep)
        assert step.name == "hitl"

    def test_build_write_to_file_step(self):
        context = get_pipeline_context()
        step = self.builder.build("write_to_file", "save", context)

        assert isinstance(step, TextWriterProcessorStep)
        assert step.name == "save"

    def test_build_unsupported_step(self):
        context = get_pipeline_context()
        with pytest.raises(ValueError, match=r"Unsupported step: unknown_step"):
            self.builder.build("unknown_step", "test_step", context)
