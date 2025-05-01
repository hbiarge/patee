import logging
from pathlib import Path

from patee import Patee, MonolingualSingleFile
from patee.steps import DoclingConfig

SAMPLES_DIR = Path(__file__).parent
PIPELINES_DIR = SAMPLES_DIR / "pipelines"
OUTPUT_DIR = SAMPLES_DIR / "outputs"

PDF_PIPELINE = PIPELINES_DIR / "from_pdf.yml"

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
    return MonolingualSingleFile(
        document_path=SAMPLES_DIR / "sources" / "Diccionari_sinonims_Espinal_a2006.pdf",
        iso2_language="ca",
        config=DoclingConfig(
            start_page=76,
            end_page=77 #1393
        )
    )

def run_pipeline(pipeline, source):
    result = pipeline.run(source, OUTPUT_DIR)

    if result.status == "succeeded":
        print("Pipeline fully executed")
    elif result.status == "stopped":
        print(f"Pipeline stopped. Reason: {result.non_succeeded_reason}")


if __name__ == '__main__':
    # Create pipeline and source
    patee = Patee.load_from(PDF_PIPELINE)
    pdf_source = create_source()

    # Run pipeline
    run_pipeline(patee, pdf_source)