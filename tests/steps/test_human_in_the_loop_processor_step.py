from pathlib import Path

from patee.step_types import StepContext
from patee.steps.human_in_the_loop_processor_step import HumanInTheLoopProcessorStep, STOP_STRING, CONTINUE_STRING
from tests.utils.mothers.contexts import get_pipeline_context, get_run_context
from tests.utils.mothers.sources import get_step_result, get_default_text_blocks

OUT_DIR = Path(__file__).parent / "out" / "human_in_the_loop_processor_step"


class TestHumanInTheLoopProcessorStep:
    def test_default_instance(self):
        context = get_pipeline_context()
        extractor = HumanInTheLoopProcessorStep("hitl", context)

        assert extractor.name == "hitl"

    def test_can_process_do_nothing_if_step_dir_contex_is_none(self):
        context = get_pipeline_context()
        extractor = HumanInTheLoopProcessorStep("hitl", context)
        pipeline_context = get_pipeline_context()
        run_context = get_run_context(output_dir=None)

        step_result = get_step_result()
        context = StepContext(
            pipeline_context=pipeline_context,
            run_context=run_context,
            step_dir=None,
        )

        result = extractor.process(context, step_result.context)

        default_text_blocks = get_default_text_blocks()
        assert result.skipped == True
        assert result.should_stop_pipeline == False
        assert result.context.document_1.text_blocks == default_text_blocks
        assert result.context.document_2.text_blocks == default_text_blocks

    def test_can_process_stops_new_if_step_dir_contex_is_not_none(self):
        context = get_pipeline_context()
        extractor = HumanInTheLoopProcessorStep("hitl", context)
        test_path: Path = OUT_DIR / "new"
        pipeline_context = get_pipeline_context()
        run_context = get_run_context(output_dir=test_path)

        step_result = get_step_result()
        test_path.mkdir(parents=True, exist_ok=True)
        context = StepContext(
            pipeline_context=pipeline_context,
            run_context=run_context,
            step_dir=test_path,
        )

        result = extractor.process(context, step_result.context)

        assert result.skipped == False
        assert result.should_stop_pipeline == True
        assert result.context is None

    def test_can_process_stops_existing_if_step_dir_contex_is_not_none(self):
        context = get_pipeline_context()
        extractor = HumanInTheLoopProcessorStep("hitl", context)
        test_path: Path = OUT_DIR / "waiting"
        pipeline_context = get_pipeline_context()
        run_context = get_run_context(output_dir=test_path)

        step_result = get_step_result()
        test_path.mkdir(parents=True, exist_ok=True)
        context = StepContext(
            pipeline_context=pipeline_context,
            run_context=run_context,
            step_dir=test_path,
        )
        step_result.context.dump_to(test_path)

        (test_path / STOP_STRING).touch()

        result = extractor.process(context, step_result.context)

        assert result.skipped == False
        assert result.should_stop_pipeline == True
        assert result.context is None

    def test_can_process_continues_if_step_dir_contex_is_not_none(self):
        context = get_pipeline_context()
        extractor = HumanInTheLoopProcessorStep("hitl", context)
        test_path: Path = OUT_DIR / "continue"
        pipeline_context = get_pipeline_context()
        run_context = get_run_context(output_dir=test_path)

        step_result = get_step_result()
        test_path.mkdir(parents=True, exist_ok=True)
        context = StepContext(
            pipeline_context=pipeline_context,
            run_context=run_context,
            step_dir=test_path,
        )
        step_result.context.dump_to(test_path)

        (test_path / CONTINUE_STRING).touch()

        result = extractor.process(context, step_result.context)

        assert result.skipped == False
        assert result.should_stop_pipeline == False
        assert result.context is not None
