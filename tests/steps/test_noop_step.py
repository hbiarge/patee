from pathlib import Path

from patee.steps import NoopStep, StepResult, LanguageResult, LanguageResultSource

TEXT_ES_FILE = Path(__file__).parent.parent / "utils" / "data" / "GUIA-PDDD_ES.pdf"
TEXT_CA_FILE = Path(__file__).parent.parent /"utils" / "data" / "GUIA-PDDD.pdf"


def test_noop_default_instance():
    extractor = NoopStep("no-op")

    assert extractor.name == "no-op"

def test_noop_can_process():
    extractor = NoopStep("no-op")

    step_result = StepResult(
        document_1=LanguageResult(
            source=LanguageResultSource(
                document_path=TEXT_ES_FILE,
                iso2_language="es",
            ),
            text="patata",
            extra={},
        ),
        document_2=LanguageResult(
            source=LanguageResultSource(
                document_path=TEXT_CA_FILE,
                iso2_language="ca",
            ),
            text="petete",
            extra={},
        ),
    )

    result = extractor.process(step_result)

    assert result.document_1.text == "patata"
    assert result.document_2.text == "petete"