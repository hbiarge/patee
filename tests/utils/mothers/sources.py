from pathlib import Path

from patee import MonolingualSingleFilePair, MonolingualSingleFile, PageInfo, SingleFile
from patee.steps import StepResult, LanguageResult, LanguageResultSource

PDF_ES_FILE = Path(__file__).parent.parent / "data" / "GUIA-PDDD_ES.pdf"
PDF_CA_FILE = Path(__file__).parent.parent / "data" / "GUIA-PDDD.pdf"
TXT_ES_FILE = Path(__file__).parent.parent / "data" / "GUIA-PDDD_ES.txt"
TXT_CA_FILE = Path(__file__).parent.parent / "data" / "GUIA-PDDD.txt"

def create_existing_single_file():
    return SingleFile(document_path=str(PDF_ES_FILE))

def get_existing_monolingual_single_file(page_info:PageInfo = None):
    if page_info:
        return MonolingualSingleFile(
            document_path=str(PDF_ES_FILE),
            iso2_language="es",
            page_info=page_info
        )

    return MonolingualSingleFile(document_path=PDF_ES_FILE, iso2_language="es")

def get_step_result():
    return StepResult(
        document_1=LanguageResult(
            source=LanguageResultSource(
                document_path=PDF_ES_FILE,
                iso2_language="es",
            ),
            text="patata",
            extra={},
        ),
        document_2=LanguageResult(
            source=LanguageResultSource(
                document_path=PDF_CA_FILE,
                iso2_language="ca",
            ),
            text="petete",
            extra={},
        ),
    )

def get_monolingual_single_file_pair(mode: str = "pdf"):
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
            shared_page_info=PageInfo(
                start_page=4,
                end_page=6,
                pages_to_exclude={5}
            )
        )