from pathlib import Path
from typing import Any

from patee.input_types import SingleFile, MonolingualSingleFile, MonolingualSingleFilePair
from patee.step_types import StepResult, DocumentSource, DocumentContext, DocumentPairContext, TextItem

SAMPLES_DIR = Path(__file__).parent.parent.parent.parent / "samples"
PIPELINES_DIR = SAMPLES_DIR / "pipelines"
SOURCES_DIR = SAMPLES_DIR / "sources"

PDF_ES_FILE = SOURCES_DIR / "GUIA-PDDD_ES.pdf"
PDF_CA_FILE = SOURCES_DIR / "GUIA-PDDD.pdf"
TXT_ES_FILE = SOURCES_DIR / "GUIA-PDDD_ES.txt"
TXT_CA_FILE = SOURCES_DIR / "GUIA-PDDD.txt"

def get_existing_pdf_file() -> Path:
    return PDF_ES_FILE

def get_existing_single_file() -> SingleFile:
    return SingleFile(document_path=str(PDF_ES_FILE))

def get_existing_monolingual_single_file(config: Any = None) -> MonolingualSingleFile:
        return MonolingualSingleFile(
            document_path=str(PDF_ES_FILE),
            iso2_language="es",
            config=config
        )

def get_existing_monolingual_single_file_pair(mode: str = "pdf") -> MonolingualSingleFilePair:
    id_pdf = mode == "pdf"
    return MonolingualSingleFilePair(
            document_1=MonolingualSingleFile(
                document_path=PDF_ES_FILE if id_pdf else TXT_ES_FILE,
                iso2_language="es",
            ),
            document_2=MonolingualSingleFile(
                document_path=PDF_CA_FILE if id_pdf else TXT_CA_FILE,
                iso2_language="ca",
            ),
            shared_config=None
        )

def get_default_text_blocks() -> list[TextItem]:
    return [
        TextItem(text="bloque de texto 1", metadata={}),
        TextItem(text="bloque de texto 2", metadata={}),
    ]

def get_existing_document_pair_context() -> DocumentPairContext:
    return DocumentPairContext(
        document_1=DocumentContext(
            source=DocumentSource(
                document_path=PDF_ES_FILE,
                iso2_language="es",
            ),
            text_blocks=get_default_text_blocks(),
            extra={},
        ),
        document_2=DocumentContext(
            source=DocumentSource(
                document_path=PDF_CA_FILE,
                iso2_language="ca",
            ),
            text_blocks=get_default_text_blocks(),
            extra={},
        ),
    )

def get_step_result():
    context = get_existing_document_pair_context()
    return StepResult(
        context=context,
    )