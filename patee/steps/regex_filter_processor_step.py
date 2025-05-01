import logging
import re

from patee.core_types import PipelineContext
from patee.step_types import (
    ParallelProcessStep,
    StepResult,
    DocumentContext,
    StepContext,
    DocumentPairContext,
)


logger = logging.getLogger(__name__)


class RegexFilterStep(ParallelProcessStep):
    def __init__(self, name: str, pipeline_context: PipelineContext, **kwargs):
        super().__init__(name, pipeline_context)

        include_regex = kwargs.get("include", None)
        if include_regex is None and not isinstance(include_regex, list):
            raise ValueError("Include must be defined and must be a list.")

        self.include = []
        for item in include_regex:
            name = item.get("name", None)
            regex = item.get("regex", None)
            if name is None or not isinstance(name, str):
                raise ValueError(f"Name must be defined and must be a string.")
            if regex is None or not isinstance(regex, str):
                raise ValueError(f"Regex for {name} must be defined and be a string.")

            self.include.append(re.compile(regex, re.RegexFlag.MULTILINE))

    @staticmethod
    def step_type() -> str:
        return "regex_filter"

    def process(self, context: StepContext, source: DocumentPairContext) -> StepResult:
        document_1_filtered = []
        for block in source.document_1.text_blocks:
            for regex in self.include:
                match = regex.search(block)
                if match is not None:
                    document_1_filtered.append(match.group())
                    break

        document_2_filtered = []
        for block in source.document_2.text_blocks:
            for regex in self.include:
                match = regex.search(block)
                if match is not None:
                    document_2_filtered.append(match.group())
                    break

        context = DocumentPairContext(
            document_1=DocumentContext(
                source=source.document_1.source,
                text_blocks=document_1_filtered,
                extra={},
            ),
            document_2=DocumentContext(
                source=source.document_2.source,
                text_blocks=document_2_filtered,
                extra={},
            )
        )
        return StepResult(
            context=context,
        )