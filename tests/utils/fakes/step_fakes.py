from typing import Union

from patee.core_types import PipelineContext
from patee.input_types import MonolingualSingleFilePair, MultilingualSingleFile
from patee.step_types import (
    ParallelExtractStep,
    StepResult,
    Step,
    DocumentSource,
    StepContext,
    ParallelProcessStep,
    DocumentContext,
    DocumentPairContext,
)
from patee.steps_builder.default_steps_builder import DefaultStepsBuilder


class FakeStepsBuilder(DefaultStepsBuilder):
    def get_supported_step_types(self) -> set[str]:
        return super().get_supported_step_types().union({"extract_fake", "text_fake"})

    def build(self, step_type: str, step_name: str, pipeline_context: PipelineContext, **kwargs) -> Step:
        try:
            return super().build(step_type, step_name, pipeline_context, **kwargs)
        except ValueError:
            if step_type == "extract_fake":
                return FakeExtractor(step_name, pipeline_context)
            elif step_type == "text_fake":
                return FakeProcessor(step_name, pipeline_context)

        raise ValueError(f"Unsupported step name: {step_type}")


class FakeExtractor(ParallelExtractStep):
    def __init__(self, name: str, pipeline_context: PipelineContext, should_stop: bool=False):
        super().__init__(name, pipeline_context)
        self.was_called = False
        self.should_stop = should_stop

    def extract(self, context: StepContext,
                source: Union[MonolingualSingleFilePair, MultilingualSingleFile]) -> StepResult:
        self.was_called = True
        if self.should_stop:
            return StepResult(
                context=None,
                should_stop_pipeline=True,
                skipped=True
            )

        if isinstance(source, MonolingualSingleFilePair):
            context = DocumentPairContext(
                document_1=DocumentContext(
                    source=DocumentSource(document_path=source.document_1.document_path,
                                          iso2_language=source.document_1.iso2_language),
                    text_blocks=["fake text 1"],
                    extra={},
                ),
                document_2=DocumentContext(
                    source=DocumentSource(document_path=source.document_2.document_path,
                                          iso2_language=source.document_2.iso2_language),
                    text_blocks=["fake text 2"],
                    extra={},
                ),
            )
            return StepResult(
                context=context,
                should_stop_pipeline=False,
                skipped=False
            )
        elif isinstance(source, MultilingualSingleFile):
            context = DocumentPairContext(
                document_1=DocumentContext(
                    source=DocumentSource(document_path=source.document_path, iso2_language=source.iso2_languages[0]),
                    text_blocks=["fake text 1"],
                    extra={},
                ),
                document_2=DocumentContext(
                    source=DocumentSource(document_path=source.document_path, iso2_language=source.iso2_languages[1]),
                    text_blocks=["fake text 2"],
                    extra={},
                ),
            )
            return StepResult(
                context=context,
            )
        else:
            raise ValueError(f"Unsupported source type: {type(source)}")


class FakeProcessor(ParallelProcessStep):
    def __init__(self, name: str, pipeline_context: PipelineContext):
        super().__init__(name, pipeline_context)
        self.was_called = False

    def process(self, context: StepContext, source: DocumentPairContext) -> StepResult:
        self.was_called = True
        context = DocumentPairContext(
            document_1=DocumentContext(
                source=source.document_1.source,
                text_blocks=[text + " fake" for text in source.document_1.text_blocks],
                extra={},
            ),
            document_2=DocumentContext(
                source=source.document_2.source,
                text_blocks=[text + " fake" for text in source.document_2.text_blocks],
                extra={},
            ),
        )
        return StepResult(
            context=context,
        )