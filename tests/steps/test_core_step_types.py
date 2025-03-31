from pathlib import Path
from unittest.mock import Mock

import pytest

from patee import MonolingualSingleFile, MultilingualSingleFile
from patee.steps import (
    Step,
    LanguageResultSource,
    LanguageResult,
    StepResult,
    ParallelExtractStep,
    ParallelProcessStep
)


class TestStep:
    def test_initialization(self):
        step_name = "test_step"
        step = Step(step_name)
        assert step.name == step_name


class TestLanguageResultSource:
    def test_initialization(self):
        path = Path("/path/to/file.pdf")
        language = "es"
        source = LanguageResultSource(path, language)

        assert source.document_path == path
        assert source.iso2_language == language

    def test_from_monolingual_file(self):
        path = Path("/path/to/file.pdf")
        language = "fr"
        mono_file = Mock(spec=MonolingualSingleFile)
        mono_file.document_path = path
        mono_file.iso2_language = language

        source = LanguageResultSource.from_monolingual_file(mono_file)

        assert source.document_path == path
        assert source.iso2_language == language

    def test_from_multilingual_file(self):
        path = Path("/path/to/multilingual.pdf")
        languages = ["en", "es"]
        multi_file = Mock(spec=MultilingualSingleFile)
        multi_file.document_path = path
        multi_file.iso2_languages = languages

        # Test with different language indexes
        source_0 = LanguageResultSource.from_multilingual_file(multi_file, 0)
        source_1 = LanguageResultSource.from_multilingual_file(multi_file, 1)

        assert source_0.document_path == path
        assert source_0.iso2_language == "en"

        assert source_1.document_path == path
        assert source_1.iso2_language == "es"


class TestLanguageResult:
    def test_initialization(self):
        source = Mock(spec=LanguageResultSource)
        source.document_path = Path("/path/to/file.pdf")
        source.iso2_language = "en"

        text = "Sample text content"
        extra = {"metadata": "test metadata"}

        result = LanguageResult(source, text, extra)

        assert result.source == source
        assert result.text == text
        assert result.extra == extra


class TestStepResult:
    def test_initialization(self):
        doc1 = Mock(spec=LanguageResult)
        doc2 = Mock(spec=LanguageResult)

        step_result = StepResult(doc1, doc2)

        assert step_result.document_1 == doc1
        assert step_result.document_2 == doc2

    def test_write_to_files(self, tmp_path):
        # Create mocked language results
        doc1 = Mock(spec=LanguageResult)
        doc2 = Mock(spec=LanguageResult)

        step_result = StepResult(doc1, doc2)
        step_result.write_to_files(tmp_path)

        # Verify both documents' write_to_file methods were called
        doc1.write_to_file.assert_called_once_with(tmp_path)
        doc2.write_to_file.assert_called_once_with(tmp_path)


class TestParallelExtractStep:
    def test_initialization(self):
        step_name = "extract_step"

        # We can't instantiate the abstract class directly, so we create a concrete subclass
        class ConcreteExtractStep(ParallelExtractStep):
            def extract(self, source):
                return Mock(spec=StepResult)

        step = ConcreteExtractStep(step_name)
        assert step.name == step_name
        assert issubclass(ConcreteExtractStep, Step)

    def test_abstract_method(self):
        # Verify that instantiating the abstract class raises TypeError
        with pytest.raises(TypeError):
            ParallelExtractStep("abstract_step")


class TestParallelProcessStep:
    def test_initialization(self):
        step_name = "process_step"

        # We can't instantiate the abstract class directly, so we create a concrete subclass
        class ConcreteProcessStep(ParallelProcessStep):
            def process(self, source):
                return Mock(spec=StepResult)

        step = ConcreteProcessStep(step_name)
        assert step.name == step_name
        assert issubclass(ConcreteProcessStep, Step)

    def test_abstract_method(self):
        # Verify that instantiating the abstract class raises TypeError
        with pytest.raises(TypeError):
            ParallelProcessStep("abstract_step")