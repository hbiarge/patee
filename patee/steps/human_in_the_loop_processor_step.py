import logging

from patee.step_types import (
    ParallelProcessStep,
    StepResult,
    StepContext,
    DocumentPairContext,
)

logger = logging.getLogger(__name__)


STOP_STRING = "rename_me_to_done_when_human_in_the_loop_is_done"
CONTINUE_STRING = "done"


class HumanInTheLoopProcessorStep(ParallelProcessStep):
    def __init__(self, name: str, **kwargs):
        super().__init__(name=name)

    @staticmethod
    def step_type() -> str:
        return "human_in_the_loop_processor"

    def process(self, context: StepContext, source: DocumentPairContext) -> StepResult:
        if context.step_dir is None:
            logger.warning(
                "skipping %s because is an human in the loop step and we are not in a persistent execution mode.",
                self.name)
            return StepResult(context=source, skipped=True)

        stop_file_path = context.step_dir / STOP_STRING
        stop_file_exists = stop_file_path.exists()
        continue_file_path = context.step_dir / CONTINUE_STRING
        continue_file_exists = continue_file_path.exists()

        if stop_file_exists:
            logger.info("stop file exists, waiting for human in the loop to finish...")
            return StepResult(
                context=None,
                should_stop_pipeline=True,
            )
        elif continue_file_exists:
            logger.info("continue file exists, continuing with the next step.")
            context = DocumentPairContext.read_from(source, context.step_dir)
            return StepResult(
                context=context,
                should_stop_pipeline=False,
            )
        else:
            logger.info("preparing human in the loop step. Creating files in step directory...")
            source.dump_to(context.step_dir)
            stop_file_path.touch()

            return StepResult(
                context=None,
                should_stop_pipeline=True,
            )
