from pathlib import Path

from patee.step_types import StepContext
from patee.steps.text_reader_extractor_step import TextReaderExtractor
from tests.utils.mothers.sources import get_existing_monolingual_single_file_pair
from tests.utils.mothers.contexts import get_pipeline_context, get_run_context

OUT_DIR = Path(__file__).parent / "out" / "text_reader_extractor"

class TestTextReaderExtractor:
    def test_text_reader_default_instance(self):
        context = get_pipeline_context()
        extractor = TextReaderExtractor("text_reader_extractor", context)

        assert extractor.name == "text_reader_extractor"

    def test_text_reader_extractor_can_process(self):
        context = get_pipeline_context()
        extractor = TextReaderExtractor("text_reader_extractor", context)
        pipeline_context = get_pipeline_context()
        run_context = get_run_context(output_dir=None)

        source = get_existing_monolingual_single_file_pair(mode="txt")
        context = StepContext(
            pipeline_context=pipeline_context,
            run_context=run_context,
            step_dir=None,
        )

        result = extractor.extract(context, source)

        OUT_DIR.mkdir(parents=True,exist_ok=True)
        result.context.dump_to(OUT_DIR)

        assert result.context.document_1.text_blocks is not None
        assert result.context.document_2.text_blocks is not None