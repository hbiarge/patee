from patee.steps import ParallelProcessStep, StepResult, LanguageResult


class NoopProcessorStep(ParallelProcessStep):

    def __init__(self, name: str, **kwargs):
        super().__init__(name=name)

    def process(self, source: StepResult) -> StepResult:
        return StepResult(
            document_1=LanguageResult(
                source=source.document_1.source,
                text=source.document_1.text,
                extra={},
            ),
            document_2=LanguageResult(
                source=source.document_2.source,
                text=source.document_2.text,
                extra={},
            )
        )