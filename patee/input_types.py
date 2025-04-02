import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Set, Union, List


@dataclass
class PageInfo:
    start_page: int = 1
    end_page: int = sys.maxsize
    pages_to_exclude: Set[int] = None

    def __key(self):
        return self.start_page, self.end_page, self.pages_to_exclude

    def __hash__(self):
        return hash(self.__key())

    def __eq__(self, other):
        if isinstance(other, PageInfo):
            return self.__key() == other.__key()
        return NotImplemented

    def __post_init__(self):
        # Validate page range
        if self.start_page < 1:
            raise ValueError(f"start_page must be at least 1, got {self.start_page}")

        if self.end_page < self.start_page:
            raise ValueError(f"end_page ({self.end_page}) must be >= start_page ({self.start_page})")

        # Initialize empty list of pages_to_exclude if None
        if self.pages_to_exclude is None:
            self.pages_to_exclude = set[int]()

        # Validate exclude_pages
        for page in self.pages_to_exclude:
            if page < 1:
                raise ValueError(f"exclude_pages must contain positive integers, got {page}")
            if page < self.start_page or page > self.end_page:
                raise ValueError(f"exclude_pages entry {page} is outside range {self.start_page}-{self.end_page}")


@dataclass()
class SingleFile:
    document_path: Union[str, Path]

    def __key(self):
        return self.document_path

    def __hash__(self):
        return hash(self.__key())

    def __eq__(self, other):
        if isinstance(other, SingleFile):
            return self.__key() == other.__key()
        return NotImplemented

    def __post_init__(self):
        # Convert string path to Path object
        if isinstance(self.document_path, str):
            self.document_path = Path(self.document_path)

        # Validate document exists and is a file
        if not self.document_path.exists():
            raise ValueError(f"Document path not found: {self.document_path}")

        if not self.document_path.is_file():
            raise ValueError(f"Document path is not a file: {self.document_path}")


@dataclass
class MonolingualSingleFile(SingleFile):
    iso2_language: str
    page_info: PageInfo = None

    def __key(self):
        return self.document_path, self.iso2_language, self.page_info

    def __hash__(self):
        return hash(self.__key())

    def __eq__(self, other):
        if isinstance(other, MonolingualSingleFile):
            return self.__key() == other.__key()
        return NotImplemented

    def __post_init__(self):
        super().__post_init__()

        # Validate language code
        if len(self.iso2_language) != 2:
            raise ValueError(f"iso2_language must be a 2-letter ISO code, got {self.iso2_language}")


@dataclass
class MonolingualSingleFilePair:
    """Represent a pair of monolingual single files processing configuration."""

    document_1: MonolingualSingleFile
    document_2: MonolingualSingleFile
    shared_page_info: PageInfo = None

    def __post_init__(self):
        # Documents should define different languages
        if self.document_1.iso2_language == self.document_2.iso2_language:
            raise ValueError("Documents must have different languages")

        # Should be just one shared page info or each document must have its own
        if self.shared_page_info is None:
            if self.document_1.page_info is None and self.document_2.page_info is None:
                # If no page info defined, create a new shared one
                self.shared_page_info = PageInfo()
            elif self.document_1.page_info is None and self.document_2.page_info is not None:
                raise ValueError("Define page information for both documents or use shared page info")
            elif self.document_1.page_info is not None and self.document_2.page_info is None:
                raise ValueError("Define page information for both documents or use shared page info")
        else:
            if self.document_1.page_info is not None or self.document_2.page_info is not None:
                raise ValueError("Define page information for both documents or use shared page info")

    def __hash__(self):
        return super().__hash__()



@dataclass
class MultilingualSingleFile(SingleFile):
    """Represent a monolingual single file processing configuration."""

    iso2_languages: List[str]
    page_info: PageInfo = None

    def __post_init__(self):
        super().__post_init__()
        # Validate language codes
        if len(self.iso2_languages) != 2:
            raise ValueError(f"iso2_languages must contain only two 2-letter ISO codes, got {self.iso2_languages}")

        for lang in self.iso2_languages:
            if len(lang) != 2:
                raise ValueError(f"iso2_languages must contain 2-letter ISO codes, got {lang}")

        unique_languages = set(lang for lang in self.iso2_languages)
        if len(unique_languages) != len(self.iso2_languages):
            raise ValueError("iso2_languages must contain unique 2-letter ISO codes")

    def __hash__(self):
        return super().__hash__()