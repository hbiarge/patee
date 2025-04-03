import logging
from dataclasses import dataclass
from typing import Union, Iterable

from docling.backend.pypdfium2_backend import PyPdfiumDocumentBackend
from docling.datamodel.base_models import InputFormat, ConversionStatus
from docling.datamodel.document import ConversionResult
from docling.datamodel.pipeline_options import PdfPipelineOptions
from docling.document_converter import DocumentConverter, PdfFormatOption
from docling_core.types.doc import NodeItem, DocItemLabel

from patee import (
    MonolingualSingleFilePair,
    MultilingualSingleFile,
    PageInfo,
    MonolingualSingleFile,
)
from patee.steps import (
    ParallelExtractStep,
    DocumentPairContext,
    StepResult,
    DocumentContext,
    DocumentSource,
    StepContext,
)

logger = logging.getLogger(__name__)


@dataclass
class _DoclingExtractionResult:
    """
    Class to hold the result of the extraction process.
    """
    extracted_text: Iterable[NodeItem]
    excluded_text: Iterable[NodeItem]
    seen_labels: set[DocItemLabel]


class DoclingExtractor(ParallelExtractStep):

    def __init__(self, name: str, **kwargs):
        super().__init__(name)

        labels_to_extract = kwargs.get("labels_to_extract", None)
        if labels_to_extract is not None:
            if isinstance(labels_to_extract, str):
                self.labels_to_extract = { labels_to_extract }
            elif isinstance(labels_to_extract, Iterable):
                self.labels_to_extract = {label for label in labels_to_extract}
            else:
                raise TypeError(f"labels_to_extract must be str or iterable of str")
        else:
            self.labels_to_extract = {str(DocItemLabel.TEXT)}

        pipeline_options = PdfPipelineOptions()
        pipeline_options.do_ocr = False

        parser = kwargs.get("parser", None)
        if parser is None or parser == "docling":
            self.parser = "docling"
            self._converter = DocumentConverter(
                format_options={
                    InputFormat.PDF: PdfFormatOption(pipeline_options=pipeline_options)
                }
            )
        elif parser == "pypdfium":
            self.parser = "pypdfium"
            self._converter = DocumentConverter(
                format_options={
                    InputFormat.PDF: PdfFormatOption(
                        pipeline_options=pipeline_options, backend=PyPdfiumDocumentBackend
                    )
                }
            )
        else:
            raise ValueError(f"Unsupported parser: {parser}. Supported parsers are 'docling' and 'pypdfium'.")

        logger.info("DocumentConverter supported formats: %s", [f.name for f in self._converter.allowed_formats])

    @staticmethod
    def step_type() -> str:
        return "docling_extractor"

    def extract(self, context: StepContext,
                source: Union[MonolingualSingleFilePair, MultilingualSingleFile]) -> StepResult:
        if isinstance(source, MonolingualSingleFilePair):
            return self._extract_file_pair(source)
        elif isinstance(source, MultilingualSingleFile):
            return self._extract_single_file(source)
        else:
            raise ValueError(f"Unsupported type: {type(source)}")


    def _extract_file_pair(self, source: MonolingualSingleFilePair) -> StepResult:
        logger.debug("converting document 1 from %s ...", source.document_1.document_path)
        document_1_result = self._convert_file(source.document_1, source.shared_config)
        logger.info("document 1 seen labels: %s", [str(label) for label in document_1_result.seen_labels])

        logger.debug("converting document 2 from %s ...",  source.document_2.document_path)
        document_2_result = self._convert_file(source.document_2, source.shared_config)
        logger.info("document 2 seen labels: %s", [str(label) for label in document_2_result.seen_labels])

        context = DocumentPairContext(
            document_1=DocumentContext(
                source=DocumentSource.from_monolingual_file(source.document_1),
                text="\n".join(element[1] for element in document_1_result.extracted_text),
                extra={
                    "excluded_text": document_1_result.excluded_text,
                    "seen_labels": [label for label in document_1_result.seen_labels]
                }
            ),
            document_2=DocumentContext(
                source=DocumentSource.from_monolingual_file(source.document_2),
                text="\n".join(element[1] for element in document_2_result.extracted_text),
                extra={
                    "excluded_text": document_2_result.excluded_text,
                    "seen_labels": [label for label in document_2_result.seen_labels]
                }
            ),
        )
        result = StepResult(
            context=context,
        )

        logger.debug("monolingual single file pairs converted successfully.")

        return result

    def _extract_single_file(self, source: MultilingualSingleFile) -> StepResult:
        raise NotImplementedError("Single file extraction is not implemented yet.")

    def _convert_file(self, file: MonolingualSingleFile, shared_page_info: PageInfo) -> _DoclingExtractionResult:
        page_range = [shared_page_info.start_page, shared_page_info.end_page] if shared_page_info \
            else [file.page_info.start_page, file.page_info.end_page] if file.page_info \
            else None
        excluded_pages = shared_page_info.pages_to_exclude if shared_page_info \
            else file.page_info.pages_to_exclude if file.page_info \
            else None

        result: ConversionResult

        if page_range is None:
            result = self._converter.convert(file.document_path)
        else:
            result = self._converter.convert(
                file.document_path,
                page_range=page_range)

        if result.status != ConversionStatus.SUCCESS:
            raise ValueError(f"Conversion failed for file {file.document_path}: {result.status}")

        extracted_text: list[(str, str)] = []
        excluded_text: list[(str, str)] = []
        seen_labels: set[DocItemLabel] = set()

        for element in result.assembled.body:
            if element.page_no + 1 not in excluded_pages:
                label = element.label
                text = element.text
                seen_labels.add(label)

                if label in self.labels_to_extract:
                    extracted_text.append((label, text))
                else:
                    excluded_text.append((label, text))

        return _DoclingExtractionResult(
            extracted_text=extracted_text,
            excluded_text=excluded_text,
            seen_labels=seen_labels
        )


