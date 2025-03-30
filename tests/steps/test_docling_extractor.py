from pathlib import Path

from patee import MonolingualSingleFilePair, MonolingualSingleFile, PageInfo
from patee.steps import DoclingExtractor

TEXT_ES_FILE = Path(__file__).parent.parent / "utils" / "data" / "GUIA-PDDD_ES.pdf"
TEXT_CA_FILE = Path(__file__).parent.parent /"utils" / "data" / "GUIA-PDDD.pdf"
OUT_DIR = Path(__file__).parent / "out"


def test_docling_default_instance():
    extractor = DoclingExtractor("docling_extractor")

    assert extractor.name == "docling_extractor"
    assert extractor.parser == "docling"
    assert extractor.labels_to_extract == { "text"}

def test_docling_instance_with_explicit_docling_parser():
    extractor = DoclingExtractor("docling_extractor", **{"parser": "docling"})

    assert extractor.name == "docling_extractor"
    assert extractor.parser == "docling"
    assert extractor.labels_to_extract == { "text"}

def test_docling_instance_with_explicit_pypdfium_parser():
    extractor = DoclingExtractor("docling_extractor", **{"parser": "pypdfium"})

    assert extractor.name == "docling_extractor"
    assert extractor.parser == "pypdfium"
    assert extractor.labels_to_extract == { "text"}

def test_docling_instance_with_explicit_extract_labels_as_string():
    extractor = DoclingExtractor("docling_extractor", **{"labels_to_extract": "list_item"})

    assert extractor.name == "docling_extractor"
    assert extractor.parser == "docling"
    assert extractor.labels_to_extract == { "list_item"}

def test_docling_instance_with_explicit_extract_labels_as_iterable():
    extractor = DoclingExtractor("docling_extractor", **{"labels_to_extract": ["text", "list_item"]})

    assert extractor.name == "docling_extractor"
    assert extractor.parser == "docling"
    assert extractor.labels_to_extract == { "text", "list_item"}

def test_docling_extractor_can_process():
    extractor = DoclingExtractor("docling_extractor")

    source = MonolingualSingleFilePair(
        document_1=MonolingualSingleFile(
            document_path=TEXT_ES_FILE,
            iso2_language="es",
        ),
        document_2=MonolingualSingleFile(
            document_path=TEXT_CA_FILE,
            iso2_language="ca",
        ),
        shared_page_info=PageInfo(
            start_page=4,
            end_page=6,
            pages_to_exclude={5}
        )
    )

    result = extractor.extract(source)

    OUT_DIR.mkdir(exist_ok=True)

    file_1 = OUT_DIR / f"{result.document_1.source.document_path.stem}.txt"
    file_2 = OUT_DIR / f"{result.document_2.source.document_path.stem}.txt"

    file_1.write_text(result.document_1.text)
    file_2.write_text(result.document_2.text)

    assert result.document_1.text is not None
    assert result.document_2.text is not None