from pathlib import Path

from patee.steps import StepContext, HumanInTheLoopProcessorStep
from patee.steps.human_in_the_loop_processor_step import STOP_STRING, CONTINUE_STRING
from tests.utils.mothers.sources import get_step_result

OUT_DIR = Path(__file__).parent / "out" / "human_in_the_loop_processor_step"


class TestHumanInTheLoopProcessorStep:
    def test_default_instance(self):
        extractor = HumanInTheLoopProcessorStep("hitl")

        assert extractor.name == "hitl"

    def test_can_process_do_nothing_if_step_dir_contex_is_none(self):
        extractor = HumanInTheLoopProcessorStep("hitl")

        step_result = get_step_result()
        context = StepContext(step_dir=None)

        result = extractor.process(context, step_result.context)

        assert result.skipped == True
        assert result.should_stop_pipeline == False
        assert result.context.document_1.text == "patata"
        assert result.context.document_2.text == "petete"

    def test_can_process_stops_new_if_step_dir_contex_is_not_none(self):
        extractor = HumanInTheLoopProcessorStep("hitl")
        test_path: Path = OUT_DIR / "new"

        step_result = get_step_result()
        test_path.mkdir(parents=True, exist_ok=True)
        context = StepContext(step_dir=test_path)

        result = extractor.process(context, step_result.context)

        assert result.skipped == False
        assert result.should_stop_pipeline == True
        assert result.context is None

    def test_can_process_stops_existing_if_step_dir_contex_is_not_none(self):
        extractor = HumanInTheLoopProcessorStep("hitl")
        test_path: Path = OUT_DIR / "waiting"

        step_result = get_step_result()
        test_path.mkdir(parents=True, exist_ok=True)
        context = StepContext(step_dir=test_path)
        step_result.context.dump_to(test_path)

        (test_path / STOP_STRING).touch()

        result = extractor.process(context, step_result.context)

        assert result.skipped == False
        assert result.should_stop_pipeline == True
        assert result.context is None

    def test_can_process_continues_if_step_dir_contex_is_not_none(self):
        extractor = HumanInTheLoopProcessorStep("hitl")
        test_path: Path = OUT_DIR / "continue"

        step_result = get_step_result()
        test_path.mkdir(parents=True, exist_ok=True)
        context = StepContext(step_dir=test_path)
        step_result.context.dump_to(test_path)

        (test_path / CONTINUE_STRING).touch()

        result = extractor.process(context, step_result.context)

        assert result.skipped == False
        assert result.should_stop_pipeline == False
        assert result.context is not None
