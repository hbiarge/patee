from pathlib import Path

from patee.steps import TextReaderExtractor, StepContext
from tests.utils.mothers.sources import get_existing_monolingual_single_file_pair

OUT_DIR = Path(__file__).parent / "out" / "text_reader_extractor"

class TestTextReaderExtractor:
    def test_text_reader_default_instance(self):
        extractor = TextReaderExtractor("text_reader_extractor")

        assert extractor.name == "text_reader_extractor"

    def test_text_reader_extractor_can_process(self):
        extractor = TextReaderExtractor("text_reader_extractor")

        source = get_existing_monolingual_single_file_pair(mode="txt")
        context = StepContext(step_dir=None)

        result = extractor.extract(context, source)

        OUT_DIR.mkdir(parents=True,exist_ok=True)
        result.context.dump_to(OUT_DIR)

        assert result.context.document_1.text is not None
        assert result.context.document_2.text is not None