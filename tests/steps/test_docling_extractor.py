import sys
from pathlib import Path

import pytest

from patee.step_types import StepContext
from patee.steps.docling_extractor_step import DoclingExtractor, DoclingConfig
from tests.utils.mothers.sources import get_existing_monolingual_single_file_pair
from tests.utils.mothers.contexts import get_pipeline_context, get_run_context

OUT_DIR = Path(__file__).parent / "out" / "docling_extractor"


class TestPageInfo:
    def test_create_default(self):
        page_info = DoclingConfig()

        assert page_info.start_page == 1
        assert page_info.end_page == sys.maxsize
        assert page_info.pages_to_exclude == set()

    def test_create_with_custom_values(self):
        page_info = DoclingConfig(start_page=5, end_page=10, pages_to_exclude={7, 8})

        assert page_info.start_page == 5
        assert page_info.end_page == 10
        assert page_info.pages_to_exclude == {7, 8}

    def test_invalid_start_page(self):
        with pytest.raises(ValueError, match="start_page must be at least 1"):
            DoclingConfig(start_page=0)

    def test_invalid_end_page(self):
        with pytest.raises(ValueError, match="end_page .* must be >= start_page"):
            DoclingConfig(start_page=10, end_page=5)

    def test_invalid_exclude_pages_negative(self):
        with pytest.raises(ValueError, match="exclude_pages must contain positive integers"):
            DoclingConfig(pages_to_exclude={-1, 5})

    def test_invalid_exclude_pages_out_of_range(self):
        with pytest.raises(ValueError, match="exclude_pages entry .* is outside range"):
            DoclingConfig(start_page=5, end_page=10, pages_to_exclude={3, 7})

    def test_equals(self):
        page_info1 = DoclingConfig(start_page=5, end_page=10, pages_to_exclude={7, 8})
        page_info2 = DoclingConfig(start_page=5, end_page=10, pages_to_exclude={7, 8})

        assert page_info1 == page_info2

    def test_non_equals(self):
        page_info1 = DoclingConfig(start_page=5, end_page=10, pages_to_exclude={7, 8})
        page_info2 = DoclingConfig(start_page=5, end_page=10, pages_to_exclude={6, 8})

        assert page_info1 != page_info2



class TestDoclingExtractor:
    def test_docling_default_instance(self):
        context = get_pipeline_context()
        extractor = DoclingExtractor("docling_extractor", context)

        assert extractor.name == "docling_extractor"
        assert extractor.parser == "docling"
        assert extractor.labels_to_extract == { "text"}

    def test_docling_instance_with_explicit_docling_parser(self):
        context = get_pipeline_context()
        extractor = DoclingExtractor("docling_extractor", context, **{"parser": "docling"})

        assert extractor.name == "docling_extractor"
        assert extractor.parser == "docling"
        assert extractor.labels_to_extract == { "text"}

    def test_docling_instance_with_explicit_pypdfium_parser(self):
        context = get_pipeline_context()
        extractor = DoclingExtractor("docling_extractor", context, **{"parser": "pypdfium"})

        assert extractor.name == "docling_extractor"
        assert extractor.parser == "pypdfium"
        assert extractor.labels_to_extract == { "text"}

    def test_docling_instance_with_explicit_extract_labels_as_string(self):
        context = get_pipeline_context()
        extractor = DoclingExtractor("docling_extractor", context, **{"labels_to_extract": "list_item"})

        assert extractor.name == "docling_extractor"
        assert extractor.parser == "docling"
        assert extractor.labels_to_extract == { "list_item"}

    def test_docling_instance_with_explicit_extract_labels_as_iterable(self):
        context = get_pipeline_context()
        extractor = DoclingExtractor("docling_extractor", context, **{"labels_to_extract": ["text", "list_item"]})

        assert extractor.name == "docling_extractor"
        assert extractor.parser == "docling"
        assert extractor.labels_to_extract == { "text", "list_item"}

    # TODO: Enable again this test when the docling extractor is fixed
    def docling_extractor_can_process(self):
        context = get_pipeline_context()
        extractor = DoclingExtractor("docling_extractor", context)
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