import re
import sys
from pathlib import Path

import pytest

from patee import PageInfo, SingleFile, MonolingualSingleFile, MonolingualSingleFilePair, MultilingualSingleFile
from tests.utils.mothers.sources import create_existing_single_file, get_existing_monolingual_single_file

# Constants for testing
TEXT_ES_FILE = Path(__file__).parent / "utils" / "data" / "GUIA-PDDD_ES.pdf"
TEXT_CA_FILE = Path(__file__).parent / "utils" / "data" / "GUIA-PDDD.pdf"


class TestPageInfo:
    def test_create_default(self):
        page_info = PageInfo()

        assert page_info.start_page == 1
        assert page_info.end_page == sys.maxsize
        assert page_info.pages_to_exclude == set()

    def test_create_with_custom_values(self):
        page_info = PageInfo(start_page=5, end_page=10, pages_to_exclude={7, 8})

        assert page_info.start_page == 5
        assert page_info.end_page == 10
        assert page_info.pages_to_exclude == {7, 8}

    def test_invalid_start_page(self):
        with pytest.raises(ValueError, match="start_page must be at least 1"):
            PageInfo(start_page=0)

    def test_invalid_end_page(self):
        with pytest.raises(ValueError, match="end_page .* must be >= start_page"):
            PageInfo(start_page=10, end_page=5)

    def test_invalid_exclude_pages_negative(self):
        with pytest.raises(ValueError, match="exclude_pages must contain positive integers"):
            PageInfo(pages_to_exclude={-1, 5})

    def test_invalid_exclude_pages_out_of_range(self):
        with pytest.raises(ValueError, match="exclude_pages entry .* is outside range"):
            PageInfo(start_page=5, end_page=10, pages_to_exclude={3, 7})

    def test_equals(self):
        page_info1 = PageInfo(start_page=5, end_page=10, pages_to_exclude={7, 8})
        page_info2 = PageInfo(start_page=5, end_page=10, pages_to_exclude={7, 8})

        assert page_info1 == page_info2

    def test_non_equals(self):
        page_info1 = PageInfo(start_page=5, end_page=10, pages_to_exclude={7, 8})
        page_info2 = PageInfo(start_page=5, end_page=10, pages_to_exclude={6, 8})

        assert page_info1 != page_info2


class TestSingleFile:
    def test_create_with_string_path(self, monkeypatch):
        # Mock Path.exists and Path.is_file to avoid file system dependencies
        monkeypatch.setattr(Path, "exists", lambda self: True)
        monkeypatch.setattr(Path, "is_file", lambda self: True)

        single_file = create_existing_single_file()
        assert isinstance(single_file.document_path, Path)
        assert single_file.document_path == TEXT_ES_FILE

    def test_create_with_path_object(self, monkeypatch):
        # Mock Path.exists and Path.is_file to avoid file system dependencies
        monkeypatch.setattr(Path, "exists", lambda self: True)
        monkeypatch.setattr(Path, "is_file", lambda self: True)

        single_file = create_existing_single_file()
        assert single_file.document_path == TEXT_ES_FILE

    def test_file_not_found(self, monkeypatch):
        monkeypatch.setattr(Path, "exists", lambda self: False)

        with pytest.raises(ValueError, match="Document path not found"):
            SingleFile(document_path="nonexistent.pdf")

    def test_path_not_a_file(self, monkeypatch):
        monkeypatch.setattr(Path, "exists", lambda self: True)
        monkeypatch.setattr(Path, "is_file", lambda self: False)

        with pytest.raises(ValueError, match="Document path is not a file"):
            SingleFile(document_path="directory/")


class TestMonolingualSingleFile:
    def test_create_valid(self, monkeypatch):
        # Mock Path.exists and Path.is_file to avoid file system dependencies
        monkeypatch.setattr(Path, "exists", lambda self: True)
        monkeypatch.setattr(Path, "is_file", lambda self: True)

        mono_file = get_existing_monolingual_single_file()
        assert mono_file.document_path == TEXT_ES_FILE
        assert mono_file.iso2_language == "es"
        assert mono_file.page_info is None

    def test_create_with_page_info(self, monkeypatch):
        # Mock Path.exists and Path.is_file to avoid file system dependencies
        monkeypatch.setattr(Path, "exists", lambda self: True)
        monkeypatch.setattr(Path, "is_file", lambda self: True)

        page_info = PageInfo(start_page=5, end_page=10)
        mono_file = get_existing_monolingual_single_file(page_info=page_info)
        assert mono_file.page_info == page_info

    def test_invalid_language_code(self, monkeypatch):
        # Mock Path.exists and Path.is_file to avoid file system dependencies
        monkeypatch.setattr(Path, "exists", lambda self: True)
        monkeypatch.setattr(Path, "is_file", lambda self: True)

        with pytest.raises(ValueError, match="iso2_language must be a 2-letter ISO code"):
            MonolingualSingleFile(document_path=TEXT_ES_FILE, iso2_language="esp")


class TestMonolingualSingleFilePair:
    def test_create_valid_with_shared_page_info(self, monkeypatch):
        # Mock Path.exists and Path.is_file to avoid file system dependencies
        monkeypatch.setattr(Path, "exists", lambda self: True)
        monkeypatch.setattr(Path, "is_file", lambda self: True)

        doc1 = MonolingualSingleFile(document_path=TEXT_ES_FILE, iso2_language="es")
        doc2 = MonolingualSingleFile(document_path=TEXT_CA_FILE, iso2_language="ca")
        page_info = PageInfo(start_page=5, end_page=10)

        pair = MonolingualSingleFilePair(
            document_1=doc1,
            document_2=doc2,
            shared_page_info=page_info
        )

        assert pair.document_1 == doc1
        assert pair.document_2 == doc2
        assert pair.shared_page_info == page_info

    def test_create_valid_with_individual_page_info(self, monkeypatch):
        # Mock Path.exists and Path.is_file to avoid file system dependencies
        monkeypatch.setattr(Path, "exists", lambda self: True)
        monkeypatch.setattr(Path, "is_file", lambda self: True)

        page_info1 = PageInfo(start_page=5, end_page=10)
        page_info2 = PageInfo(start_page=6, end_page=12)

        doc1 = MonolingualSingleFile(document_path=TEXT_ES_FILE, iso2_language="es", page_info=page_info1)
        doc2 = MonolingualSingleFile(document_path=TEXT_CA_FILE, iso2_language="ca", page_info=page_info2)

        pair = MonolingualSingleFilePair(
            document_1=doc1,
            document_2=doc2
        )

        assert pair.shared_page_info is None

    def test_create_default_shared_page_info(self, monkeypatch):
        # Mock Path.exists and Path.is_file to avoid file system dependencies
        monkeypatch.setattr(Path, "exists", lambda self: True)
        monkeypatch.setattr(Path, "is_file", lambda self: True)

        doc1 = MonolingualSingleFile(document_path=TEXT_ES_FILE, iso2_language="es")
        doc2 = MonolingualSingleFile(document_path=TEXT_CA_FILE, iso2_language="ca")

        pair = MonolingualSingleFilePair(
            document_1=doc1,
            document_2=doc2
        )

        assert pair.shared_page_info is not None
        assert pair.shared_page_info.start_page == 1
        assert pair.shared_page_info.end_page == sys.maxsize

    def test_same_language_error(self, monkeypatch):
        # Mock Path.exists and Path.is_file to avoid file system dependencies
        monkeypatch.setattr(Path, "exists", lambda self: True)
        monkeypatch.setattr(Path, "is_file", lambda self: True)

        doc1 = MonolingualSingleFile(document_path=TEXT_ES_FILE, iso2_language="es")
        doc2 = MonolingualSingleFile(document_path=TEXT_CA_FILE, iso2_language="es")

        with pytest.raises(ValueError, match="Documents must have different languages"):
            MonolingualSingleFilePair(document_1=doc1, document_2=doc2)

    def test_mixed_page_info_error_one_missing(self, monkeypatch):
        # Mock Path.exists and Path.is_file to avoid file system dependencies
        monkeypatch.setattr(Path, "exists", lambda self: True)
        monkeypatch.setattr(Path, "is_file", lambda self: True)

        page_info = PageInfo(start_page=5, end_page=10)
        doc1 = MonolingualSingleFile(document_path=TEXT_ES_FILE, iso2_language="es", page_info=page_info)
        doc2 = MonolingualSingleFile(document_path=TEXT_CA_FILE, iso2_language="ca")

        with pytest.raises(ValueError, match="Define page information for both documents or use shared page info"):
            MonolingualSingleFilePair(document_1=doc1, document_2=doc2)

    def test_mixed_page_info_error_with_shared(self, monkeypatch):
        # Mock Path.exists and Path.is_file to avoid file system dependencies
        monkeypatch.setattr(Path, "exists", lambda self: True)
        monkeypatch.setattr(Path, "is_file", lambda self: True)

        page_info = PageInfo(start_page=5, end_page=10)
        doc1 = MonolingualSingleFile(document_path=TEXT_ES_FILE, iso2_language="es", page_info=page_info)
        doc2 = MonolingualSingleFile(document_path=TEXT_CA_FILE, iso2_language="ca")

        with pytest.raises(ValueError, match="Define page information for both documents or use shared page info"):
            MonolingualSingleFilePair(
                document_1=doc1,
                document_2=doc2,
                shared_page_info=page_info
            )


class TestMultilingualSingleFile:
    def test_create_valid(self, monkeypatch):
        # Mock Path.exists and Path.is_file to avoid file system dependencies
        monkeypatch.setattr(Path, "exists", lambda self: True)
        monkeypatch.setattr(Path, "is_file", lambda self: True)

        multi_file = MultilingualSingleFile(
            document_path=TEXT_ES_FILE,
            iso2_languages=["es", "ca"]
        )
        assert multi_file.document_path == TEXT_ES_FILE
        assert multi_file.iso2_languages == ["es", "ca"]
        assert multi_file.page_info is None

    def test_create_with_page_info(self, monkeypatch):
        # Mock Path.exists and Path.is_file to avoid file system dependencies
        monkeypatch.setattr(Path, "exists", lambda self: True)
        monkeypatch.setattr(Path, "is_file", lambda self: True)

        page_info = PageInfo(start_page=5, end_page=10)
        multi_file = MultilingualSingleFile(
            document_path=TEXT_ES_FILE,
            iso2_languages=["es", "ca"],
            page_info=page_info
        )
        assert multi_file.page_info == page_info

    def test_invalid_language_code_number(self, monkeypatch):
        # Mock Path.exists and Path.is_file to avoid file system dependencies
        monkeypatch.setattr(Path, "exists", lambda self: True)
        monkeypatch.setattr(Path, "is_file", lambda self: True)

        with pytest.raises(ValueError, match=re.escape("iso2_languages must contain only two 2-letter ISO codes, got ['es', 'ca', 'en']")):
            MultilingualSingleFile(
                document_path=TEXT_ES_FILE,
                iso2_languages=["es", "ca", "en"]
            )

    def test_invalid_language_code(self, monkeypatch):
        # Mock Path.exists and Path.is_file to avoid file system dependencies
        monkeypatch.setattr(Path, "exists", lambda self: True)
        monkeypatch.setattr(Path, "is_file", lambda self: True)

        with pytest.raises(ValueError, match="iso2_languages must contain 2-letter ISO codes"):
            MultilingualSingleFile(
                document_path=TEXT_ES_FILE,
                iso2_languages=["es", "cat"]
            )

    def test_duplicate_language_codes(self, monkeypatch):
        # Mock Path.exists and Path.is_file to avoid file system dependencies
        monkeypatch.setattr(Path, "exists", lambda self: True)
        monkeypatch.setattr(Path, "is_file", lambda self: True)

        with pytest.raises(ValueError, match="iso2_languages must contain unique 2-letter ISO codes"):
            MultilingualSingleFile(
                document_path=TEXT_ES_FILE,
                iso2_languages=["es", "es"]
            )