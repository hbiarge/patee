from pathlib import Path

from patee.input_types import SingleFile, MonolingualSingleFile, MonolingualSingleFilePair, PageInfo
from patee.step_types import StepResult, DocumentSource, DocumentContext, DocumentPairContext

SAMPLES_DIR = Path(__file__).parent.parent.parent.parent / "samples"
PIPELINES_DIR = SAMPLES_DIR / "pipelines"
SOURCES_DIR = SAMPLES_DIR / "sources"

PDF_ES_FILE = SOURCES_DIR / "GUIA-PDDD_ES.pdf"
PDF_CA_FILE = SOURCES_DIR / "GUIA-PDDD.pdf"
TXT_ES_FILE = SOURCES_DIR / "GUIA-PDDD_ES.txt"
TXT_CA_FILE = SOURCES_DIR / "GUIA-PDDD.txt"

def get_existing_pdf_file():
    return PDF_ES_FILE

def get_existing_single_file():
    return SingleFile(document_path=str(PDF_ES_FILE))

def get_existing_monolingual_single_file(page_info:PageInfo = None):
    if page_info:
        return MonolingualSingleFile(
            document_path=str(PDF_ES_FILE),
            iso2_language="es",
            page_info=page_info
        )

    return MonolingualSingleFile(document_path=PDF_ES_FILE, iso2_language="es")

def get_existing_monolingual_single_file_pair(mode: str = "pdf"):
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
            shared_config=PageInfo(
                start_page=4,
                end_page=6,
                pages_to_exclude={5}
            )
        )

def get_existing_document_pair_context():
    return DocumentPairContext(
        document_1=DocumentContext(
            source=DocumentSource(
                document_path=PDF_ES_FILE,
                iso2_language="es",
            ),
            text="patata",
            extra={},
        ),
        document_2=DocumentContext(
            source=DocumentSource(
                document_path=PDF_CA_FILE,
                iso2_language="ca",
            ),
            text="petete",
            extra={},
        ),
    )

def get_step_result():
    context = get_existing_document_pair_context()
    return StepResult(
        context=context,
    )