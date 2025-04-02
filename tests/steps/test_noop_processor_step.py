from pathlib import Path

from patee.steps import NoopProcessorStep, StepContext
from tests.utils.mothers.sources import get_step_result

OUT_DIR = Path(__file__).parent / "out" / "noop_processor_step"


class TestNoopProcessorStep:
    def test_noop_default_instance(self):
        extractor = NoopProcessorStep("no-op")

        assert extractor.name == "no-op"

    def test_noop_can_process(self):
        extractor = NoopProcessorStep("no-op")

        step_result = get_step_result()
        context = StepContext(step_dir=None)

        result = extractor.process(context, step_result.context)

        OUT_DIR.mkdir(exist_ok=True)
        result.context.dump_to(OUT_DIR)

        assert result.context.document_1.text == "patata"
        assert result.context.document_2.text == "petete"