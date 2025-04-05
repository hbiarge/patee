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
                text_blocks=source.document_1.text_blocks,
                extra={},
            ),
            document_2=DocumentContext(
                source=source.document_2.source,
                text_blocks=source.document_2.text_blocks,
                extra={},
            )
        )
        return StepResult(
            context=context,
        )