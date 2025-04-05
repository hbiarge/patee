from pathlib import Path

from patee.step_types import StepContext
from patee.steps.noop_processor_step import NoopProcessorStep
from tests.utils.mothers.sources import get_step_result
from tests.utils.mothers.contexts import get_pipeline_context, get_run_context

OUT_DIR = Path(__file__).parent / "out" / "noop_processor_step"


class TestNoopProcessorStep:
    def test_noop_default_instance(self):
        extractor = NoopProcessorStep("no-op")

        assert extractor.name == "no-op"

    def test_noop_can_process(self):
        extractor = NoopProcessorStep("no-op")
        pipeline_context = get_pipeline_context()
        run_context = get_run_context(output_dir=None)

        step_result = get_step_result()
        context = StepContext(
            pipeline_context=pipeline_context,
            run_context=run_context,
            step_dir=None,
        )

        result = extractor.process(context, step_result.context)

        OUT_DIR.mkdir(parents=True,exist_ok=True)
        result.context.dump_to(OUT_DIR)

        assert result.context.document_1.text == "patata"
        assert result.context.document_2.text == "petete"