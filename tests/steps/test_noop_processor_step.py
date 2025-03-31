from patee.steps import NoopProcessorStep
from tests.utils.mothers.sources import get_step_result


class TestNoopProcessorStep:
    def test_noop_default_instance(self):
        extractor = NoopProcessorStep("no-op")

        assert extractor.name == "no-op"

    def test_noop_can_process(self):
        extractor = NoopProcessorStep("no-op")

        step_result = get_step_result()

        result = extractor.process(step_result)

        assert result.document_1.text == "patata"
        assert result.document_2.text == "petete"