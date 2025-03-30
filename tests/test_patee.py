from pathlib import Path

from patee import Patee, MonolingualSingleFilePair, MonolingualSingleFile, PageInfo
from tests.fakes.step_fakes import FakeStepsBuilder

EXTRACT_ONLY_CONFIG = Path(__file__).parent / "pipeline_samples" / "extract_only.yml"
FAKES_CONFIG = Path(__file__).parent / "pipeline_samples" / "fakes.yml"

TEXT_ES_FILE = Path(__file__).parent / "utils" / "data" / "GUIA-PDDD_ES.pdf"
TEXT_CA_FILE = Path(__file__).parent / "utils" / "data" / "GUIA-PDDD.pdf"


def test_load_with_default_builder_patee():
    patee = Patee.load_from(EXTRACT_ONLY_CONFIG)

    assert patee.step_names == ["00_parse"]

def test_load_with_fake_builder_patee():
    builder = FakeStepsBuilder()
    patee = Patee.load_from(FAKES_CONFIG, steps_builder=builder)

    assert patee.step_names == ["00_extract", "01_process"]


def test_patee_can_remove_steps():
    builder = FakeStepsBuilder()
    patee = Patee.load_from(FAKES_CONFIG, steps_builder=builder)
    patee.remove_step("00_extract")

    assert patee.step_names == ["01_process"]

def test_patee_is_valid():
    builder = FakeStepsBuilder()
    patee = Patee.load_from(FAKES_CONFIG, steps_builder=builder)

    assert patee.is_valid == True


def test_patee_can_process():
    builder = FakeStepsBuilder()
    patee = Patee.load_from(FAKES_CONFIG, steps_builder=builder)

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

    result = patee.process(source, out_dir=Path(__file__).parent / "out")

    assert result.document_1 is not None
    assert result.document_2 is not None