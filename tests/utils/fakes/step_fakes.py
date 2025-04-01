from typing import Union, Iterable

from patee import MonolingualSingleFilePair, MultilingualSingleFile
from patee.steps_builder import DefaultStepsBuilder
from patee.steps import ParallelExtractStep, StepResult, Step, LanguageResultSource
from patee.steps.core_step_types import ParallelProcessStep, LanguageResult


class FakeStepsBuilder(DefaultStepsBuilder):
    def get_supported_step_types(self) -> set[str]:
        return super().get_supported_step_types().union({"extract_fake", "text_fake"})

    def build(self, step_type: str, step_name:str, **kwargs) -> Step:
        try:
            return super().build(step_type, step_name, **kwargs)
        except ValueError:
            if step_type == "extract_fake":
                return FakeExtractor(step_name)
            elif step_type == "text_fake":
                return FakeProcessor(step_name)

        raise ValueError(f"Unsupported step name: {step_type}")


class FakeExtractor(ParallelExtractStep):
    def __init__(self, name: str):
        super().__init__(name)

    def extract(self, source: Union[MonolingualSingleFilePair, MultilingualSingleFile]) -> StepResult:
        if isinstance(source, MonolingualSingleFilePair):
            return StepResult(
                document_1=LanguageResult(
                    source=LanguageResultSource(document_path=source.document_1.document_path, iso2_language=source.document_1.iso2_language),
                    text="fake text 1",
                    extra={},
                ),
                document_2=LanguageResult(
                    source=LanguageResultSource(document_path=source.document_2.document_path, iso2_language=source.document_2.iso2_language),
                    text="fake text 2",
                    extra={},
                ),
            )
        elif isinstance(source, MultilingualSingleFile):
            return StepResult(
                document_1=LanguageResult(
                    source=LanguageResultSource(document_path=source.document_path, iso2_language=source.iso2_languages[0]),
                    text="fake text 1",
                    extra={},
                ),
                document_2=LanguageResult(
                    source=LanguageResultSource(document_path=source.document_path, iso2_language=source.iso2_languages[1]),
                    text="fake text 2",
                    extra={},
                ),
            )
        else:
            raise ValueError(f"Unsupported source type: {type(source)}")


class FakeProcessor(ParallelProcessStep):
    def __init__(self, name: str):
        super().__init__(name)

    def process(self, source: StepResult) -> StepResult:
        return StepResult(
            document_1=LanguageResult(
                source=source.document_1.source,
                text=source.document_1.text + " fake",
                extra={},
            ),
            document_2=LanguageResult(
                source=source.document_2.source,
                text=source.document_2.text + " fake",
                extra={},
            )
        )