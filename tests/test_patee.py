from pathlib import Path

from patee import Patee, MonolingualSingleFilePair, MonolingualSingleFile, PageInfo
from tests.utils.fakes.step_fakes import FakeStepsBuilder
from tests.utils.mothers.sources import get_monolingual_single_file_pair

EXTRACT_ONLY_CONFIG = Path(__file__).parent / "pipeline_samples" / "extract_only.yml"
FAKES_CONFIG = Path(__file__).parent / "pipeline_samples" / "fakes.yml"

OUT_DIR = Path(__file__).parent / "out"

class TestPatee:
    def test_load_with_default_builder(self):
        patee = Patee.load_from(EXTRACT_ONLY_CONFIG)

        assert patee.step_names == ["00_parse"]

    def test_load_with_fake_builder_patee(self):
        builder = FakeStepsBuilder()
        patee = Patee.load_from(FAKES_CONFIG, steps_builder=builder)

        assert patee.step_names == ["00_extract", "01_process"]


    def test_patee_can_remove_steps(self):
        builder = FakeStepsBuilder()
        patee = Patee.load_from(FAKES_CONFIG, steps_builder=builder)
        patee.remove_step("00_extract")

        assert patee.step_names == ["01_process"]

    def test_patee_can_process_without_out_dir(self):
        builder = FakeStepsBuilder()
        patee = Patee.load_from(FAKES_CONFIG, steps_builder=builder)

        source = get_monolingual_single_file_pair()

        result = patee.process(source)

        assert result.document_1 is not None
        assert result.document_2 is not None

    def test_patee_can_process_with_out_dir(self):
        builder = FakeStepsBuilder()
        patee = Patee.load_from(FAKES_CONFIG, steps_builder=builder)

        source = get_monolingual_single_file_pair()

        OUT_DIR.mkdir(parents=True, exist_ok=True)

        result = patee.process(source, OUT_DIR)

        assert result.document_1 is not None
        assert result.document_2 is not None