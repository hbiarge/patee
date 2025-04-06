from pathlib import Path
from typing import Union

from patee.core_types import PipelineContext, RunContext

SAMPLES_DIR = Path(__file__).parent.parent.parent.parent / "samples"
PIPELINES_DIR = SAMPLES_DIR / "pipelines"
SOURCES_DIR = SAMPLES_DIR / "sources"


def get_pipeline_context():
    return PipelineContext(
        config_path=PIPELINES_DIR / "pdf.yml",
        execution_path=SAMPLES_DIR.parent / "tests",
    )

def get_run_context(output_dir: Union[Path, None], source_hash: str = "123456"):
    return RunContext(output_dir=output_dir, source_hash=source_hash)