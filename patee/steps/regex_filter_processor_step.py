import logging
import re

from patee.core_types import PipelineContext
from patee.step_types import (
    ParallelProcessStep,
    StepResult,
    DocumentContext,
    StepContext,
    DocumentPairContext, TextItem,
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

    def _process_document(self, context: StepContext, source: DocumentContext) -> StepResult:
        filtered_blocks = self._filter_document_blocks(source.text_blocks)

        context = DocumentContext(
            source=source.source,
            text_blocks=filtered_blocks,
            extra={},
        )
        return StepResult(
            context=context,
        )


    def _process_document_pair(self, context: StepContext, source: DocumentPairContext) -> StepResult:
        document_1_filtered = self._filter_document_blocks(source.document_1.text_blocks)
        document_2_filtered = self._filter_document_blocks(source.document_2.text_blocks)

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

    def _filter_document_blocks(self, original_blocks: list[TextItem]) -> list[TextItem]:
        filtered_blocks: list[TextItem] = []

        for block in original_blocks:
            for regex in self.include:
                match = regex.search(block.text)
                if match is not None:
                    if match.lastindex is not None:
                        groups = match.groups()
                        item = TextItem(text=groups[0], metadata=block.metadata.copy())
                        item.metadata[f"filter:{self.name}:original"] = block.text
                        for idx in range(1, match.lastindex):
                            item.metadata[f"filter:{self.name}:group_{idx}"] = groups[idx]
                        filtered_blocks.append(item)
                        break
                    else:
                        item = TextItem(text=match.group(), metadata=block.metadata.copy())
                        item.metadata[f"filter:{self.name}:original"] = block.text
                        filtered_blocks.append(item)
                        break

        return filtered_blocks
