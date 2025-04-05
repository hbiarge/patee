import logging

from patee.step_types import (
    ParallelProcessStep,
    StepResult,
    DocumentContext,
    StepContext,
    DocumentPairContext,
)


logger = logging.getLogger(__name__)


class NoopProcessorStep(ParallelProcessStep):
    def __init__(self, name: str, **kwargs):
        super().__init__(name=name)

    @staticmethod
    def step_type() -> str:
        return "noop_step_processor"

    def process(self, context: StepContext, source: DocumentPairContext) -> StepResult:
        context = DocumentPairContext(
            document_1=DocumentContext(
                source=source.document_1.source,
                text=source.document_1.text,
                extra={},
            ),
            document_2=DocumentContext(
                source=source.document_2.source,
                text=source.document_2.text,
                extra={},
            )
        )
        return StepResult(
            context=context,
        )