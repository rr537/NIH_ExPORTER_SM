from datetime import datetime
from pathlib import Path

date_string = datetime.today().strftime("%Y-%m-%d")

# --- Derived paths based on config ---
configfile: "config/config.yaml"

# --- Define paths ---
CONFIG_PATH = "config/config.yaml"

rule preprocess:
    input:
        config = CONFIG_PATH
    output:
        preprocessed_dir = directory(config['preprocessed_dir'])
    params:
        summary = str(Path(config['preprocessed_dir']) / "preprocess_summary.json")
    log:
        f"logs/preprocess_{date_string}.log"
    conda:
        "envs/nih.yml"
    shell:
        """
        python scripts/cli.py preprocess \
            --config {input.config} \
            --output {output.preprocessed_dir} \
            --summary-json {params.summary} \
            > {log} 2>&1
        """

# Preprocess
PREPROCESSED_DIR = Path(config["preprocessed_dir"])
PICKLE_FILES = list(PREPROCESSED_DIR.glob("*.pkl"))
if not PICKLE_FILES:
    raise FileNotFoundError("No pickle files found in preprocessed directory.")

rule metrics:
    input:
        config = CONFIG_PATH,
        pickles = PICKLE_FILES
    output:
        metrics_dir = directory(config['metrics_dir']),
        summary = lambda wildcards, output: f"{output.metrics_dir}/metrics_summary.json"
    log:
        f"logs/metrics_{date_string}.log"
    conda:
        "envs/nih.yml"
    shell:
        """
        python scripts/cli.py metrics --pickles {input.pickles} --config {input.config} --output {output.metrics_dir} --summary-json {output.summary} > {log} 2>&1
        """

# Metrics
METRICS_DIR = Path(config["metrics_dir"])
METRICS_FILE = sorted(METRICS_DIR.glob("*.csv"))
if not METRICS_FILE:
    raise FileNotFoundError("No metrics CSV file found in directory.")
METRICS_FILE = METRICS_FILE[0] # take the most recent metrics file

rule keywords:
    input:
        config = CONFIG_PATH,
        metrics = METRICS_FILE
    output:
        keywords_dir = directory(config['keywords_dir']),
        summary = lambda wildcards, output: f"{output.keywords_dir}/keywords_summary.json"
    log:
        f"logs/keywords_{date_string}{'_stopwords' if config.get('use_stopwords', False) else ''}.log"
    params:
        stopwords_flag = "--stopwords" if config.get("use_stopwords", False) else ""
    conda:
        "envs/nih.yml"
    shell:
        """
        python scripts/cli.py keywords \
            --metrics {input.metrics} \
            --config {input.config} \
            --output {output.keywords_dir} \
            --summary-json {output.summary} \
            --log-file {log} \
            {params.stopwords_flag} \
            > {log} 2>&1
        """

# Keywords
KEYWORDS_DIR = Path(config["keywords_dir"])
KEYWORDS_FILE = sorted(KEYWORDS_DIR.glob("*.csv"))
if not KEYWORDS_FILE:
    raise FileNotFoundError("No keywords CSV file found in directory.")
KEYWORDS_FILE = KEYWORDS_FILE[0] # take the most recent metrics file


rule finalize:
    input:
        config = CONFIG_PATH,
        keywords = KEYWORDS_FILE
    output:
        finalize_dir = directory(config['finalize_dir']),
        summary = lambda wildcards, output: f"{output.finalize_dir}/finalize_summary.json",
        dropped = lambda wildcards, output: f"{output.finalize_dir}/dropped_rows.csv" if config.get("finalize_drop_output", False) else None
    log:
         f"logs/finalize_{date_string}.log"
    params:
        drop_flag = "--drop-output" if config.get("finalize_drop_output", False) else ""
    conda:
        "envs/nih.yml"
    shell:
        """
        python scripts/cli.py finalize \
            --keywords {input.keywords} \
            --config {input.config} \
            --output {output.finalize_dir} \
            --summary-json {output.summary} \
            --log-file {log} \
            {params.drop_flag} \
            > {log} 2>&1
        """
