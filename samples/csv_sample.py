import logging
from pathlib import Path

from patee import Patee, MultilingualSingleFile
from patee.steps import MultilingualFileCsvConfig

SAMPLES_DIR = Path(__file__).parent
PIPELINES_DIR = SAMPLES_DIR / "pipelines"
OUTPUT_DIR = SAMPLES_DIR / "outputs"

CSV_PIPELINE = PIPELINES_DIR / "from_csv.yml"

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


def create_source():
    config = MultilingualFileCsvConfig(
        language_1_idx=4,
        language_2_idx=5,
        options={
            "header": 0,
            "sep": "\t",
        }
    )

    return MultilingualSingleFile(
        document_path=SAMPLES_DIR / "sources" / "idioms_sentences.tsv",
        iso2_languages=["en", "es"],
        config=config,
    )

def run_pipeline(pipeline, source):
    result = pipeline.run(source, OUTPUT_DIR)

    if result.status == "succeeded":
        print("Pipeline fully executed")
    elif result.status == "stopped":
        print(f"Pipeline stopped. Reason: {result.non_succeeded_reason}")


if __name__ == '__main__':
    # Create pipeline and source
    patee = Patee.load_from(CSV_PIPELINE)
    csv_source = create_source()

    # Run pipeline
    run_pipeline(patee, csv_source)