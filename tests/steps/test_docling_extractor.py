from pathlib import Path

from patee.steps import DoclingExtractor
from tests.utils.mothers.sources import get_monolingual_single_file_pair

OUT_DIR = Path(__file__).parent / "out"

class TestDoclingExtractor:
    def test_docling_default_instance(self):
        extractor = DoclingExtractor("docling_extractor")

        assert extractor.name == "docling_extractor"
        assert extractor.parser == "docling"
        assert extractor.labels_to_extract == { "text"}

    def test_docling_instance_with_explicit_docling_parser(self):
        extractor = DoclingExtractor("docling_extractor", **{"parser": "docling"})

        assert extractor.name == "docling_extractor"
        assert extractor.parser == "docling"
        assert extractor.labels_to_extract == { "text"}

    def test_docling_instance_with_explicit_pypdfium_parser(self):
        extractor = DoclingExtractor("docling_extractor", **{"parser": "pypdfium"})

        assert extractor.name == "docling_extractor"
        assert extractor.parser == "pypdfium"
        assert extractor.labels_to_extract == { "text"}

    def test_docling_instance_with_explicit_extract_labels_as_string(self):
        extractor = DoclingExtractor("docling_extractor", **{"labels_to_extract": "list_item"})

        assert extractor.name == "docling_extractor"
        assert extractor.parser == "docling"
        assert extractor.labels_to_extract == { "list_item"}

    def test_docling_instance_with_explicit_extract_labels_as_iterable(self):
        extractor = DoclingExtractor("docling_extractor", **{"labels_to_extract": ["text", "list_item"]})

        assert extractor.name == "docling_extractor"
        assert extractor.parser == "docling"
        assert extractor.labels_to_extract == { "text", "list_item"}

    def test_docling_extractor_can_process(self):
        extractor = DoclingExtractor("docling_extractor")

        source = get_monolingual_single_file_pair()

        result = extractor.extract(source)

        OUT_DIR.mkdir(exist_ok=True)
        result.write_to_files(OUT_DIR)

        assert result.document_1.text is not None
        assert result.document_2.text is not None