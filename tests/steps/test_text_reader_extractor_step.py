from pathlib import Path

from patee.steps import TextReaderExtractor
from tests.utils.mothers.sources import get_monolingual_single_file_pair

OUT_DIR = Path(__file__).parent / "out"

class TestTextReaderExtractor:
    def test_text_reader_default_instance(self):
        extractor = TextReaderExtractor("text_reader_extractor")

        assert extractor.name == "text_reader_extractor"

    def test_text_reader_extractor_can_process(self):
        extractor = TextReaderExtractor("text_reader_extractor")

        source = get_monolingual_single_file_pair(mode="txt")

        result = extractor.extract(source)

        OUT_DIR.mkdir(exist_ok=True)
        result.write_to_files(OUT_DIR)

        assert result.document_1.text is not None
        assert result.document_2.text is not None