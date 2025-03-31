from pathlib import Path

from patee import MonolingualSingleFilePair, MonolingualSingleFile, PageInfo
from patee.steps import TextReaderExtractor

TEXT_ES_FILE = Path(__file__).parent.parent / "utils" / "data" / "GUIA-PDDD_ES.txt"
TEXT_CA_FILE = Path(__file__).parent.parent /"utils" / "data" / "GUIA-PDDD.txt"
OUT_DIR = Path(__file__).parent / "out"

class TestTextReaderExtractor:
    def test_text_reader_default_instance(self):
        extractor = TextReaderExtractor("text_reader_extractor")

        assert extractor.name == "text_reader_extractor"

    def test_text_reader_extractor_can_process(self):
        extractor = TextReaderExtractor("text_reader_extractor")

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