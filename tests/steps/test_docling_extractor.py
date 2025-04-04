from pathlib import Path

from patee.step_types import StepContext
from patee.steps.docling_extractor_step import DoclingExtractor
from tests.utils.mothers.sources import get_existing_monolingual_single_file_pair
from tests.utils.mothers.contexts import get_pipeline_context, get_run_context

OUT_DIR = Path(__file__).parent / "out" / "docling_extractor"

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
        pipeline_context = get_pipeline_context()
        run_context = get_run_context(output_dir=None)

        source = get_existing_monolingual_single_file_pair()
        context = StepContext(
            pipeline_context=pipeline_context,
            run_context=run_context,
            step_dir=None
        )

        result = extractor.extract(context, source)

        OUT_DIR.mkdir(parents=True,exist_ok=True)
        result.context.dump_to(OUT_DIR)

        assert result.context.document_1.text_blocks is not None
        assert result.context.document_2.text_blocks is not None