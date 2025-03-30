from typing import Union

from patee import MonolingualSingleFilePair, MultilingualSingleFile
from patee.patee import StepBuilder
from patee.steps import ParallelExtractStep, StepResult, Step, LanguageResultSource
from patee.steps.core_step_types import ParallelTextStep, LanguageResult


class FakeStepBuilder(StepBuilder):

    def build(self, step_type: str, step_name:str, **kwargs) -> Step:
        if step_type == "extract_fake":
            return ExtractFake(step_name)
        elif step_type == "text_fake":
            return TextFake(step_name)
        else:
            raise ValueError(f"Unknown step name: {step_type}")


class ExtractFake(ParallelExtractStep):
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


class TextFake(ParallelTextStep):
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