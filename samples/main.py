import logging
from pathlib import Path

from patee import Patee, MonolingualSingleFilePair, MonolingualSingleFile, PageInfo

SAMPLES_DIR = Path(__file__).parent

# Set DEBUG level for patee
logging.basicConfig(level=logging.DEBUG)
logging.getLogger("patee").setLevel(logging.DEBUG)

# Define log level for other libraries
libraries_log_levels = {
    "docling": logging.WARNING,
    "docling_ibm_models": logging.WARNING,
    "urllib3": logging.WARNING,
}

for name, level in libraries_log_levels.items():
    logging.getLogger(name).setLevel(level)


def create_pipeline() -> Patee:
    config = SAMPLES_DIR / "pipelines" / "pdf.yml"
    pipeline = Patee.load_from(config)

    print("patee steps: ", pipeline.step_names)

    return pipeline

def create_source():
    document_1 = MonolingualSingleFile(
            document_path=SAMPLES_DIR / "sources" / "GUIA-PDDD_ES.pdf",
            iso2_language="es",
        )
    document_2 = MonolingualSingleFile(
            document_path=SAMPLES_DIR / "sources" / "GUIA-PDDD.pdf",
            iso2_language="ca",
        )
    config = PageInfo(
            start_page=4,
            end_page=5
        )
    source = MonolingualSingleFilePair(
        document_1=document_1,
        document_2=document_2,
        shared_config=config,
    )

    print("source: ", source)

    return source

def run_pipeline(pipeline: Patee, source: MonolingualSingleFilePair):
    result = pipeline.run(source)

    if result.status == "succeeded":
        print("Pipeline fully executed")
    elif result.status == "stopped":
        print(f"Pipeline stopped. Reason: {result.non_succeeded_reason}")


if __name__ == '__main__':
    # Create pipeline and source
    patee = create_pipeline()
    current_source = create_source()

    # Run pipeline
    run_pipeline(patee, current_source)