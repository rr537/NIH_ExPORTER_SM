from datetime import datetime

date_string = datetime.today().strftime("%Y-%m-%d")

# --- Derived paths based on config ---
configfile: "config/config.yaml"

# --- Define paths ---
CONFIG_PATH = "config/config.yaml"

rule preprocess:
    input:
        config = CONFIG_PATH
    output:
        preprocessed_dir = directory(config["preprocessed_dir"])
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

rule metrics:
    input:
        config = CONFIG_PATH,
        pickles = config["preprocessed_dir"]
    output:
        metrics_dir = directory(config['metrics_dir']),
        summary = str(Path(config['metrics_dir']) / "metrics_summary.json")
    log:
        f"logs/metrics_{date_string}.log"
    conda:
        "envs/nih.yml"
    shell:
        """
        python scripts/cli.py metrics --pickles {input.pickles} --config {input.config} --output {output.metrics_dir} --summary-json {output.summary} > {log} 2>&1
        """

rule keywords:
    input:
        config = CONFIG_PATH,
        metrics = config["metrics_dir"]
    output:
        keywords_dir = directory(config['keywords_dir']),
        summary = str(Path(config['keywords_dir']) / "keywords_summary.json")
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
            {params.stopwords_flag} \
            > {log} 2>&1
        """

rule finalize:
    input:
        config = CONFIG_PATH,
        keywords = config["keywords_dir"]
    output:
        finalize_dir = directory(config['finalize_dir']),
        summary = str(Path(config['finalize_dir']) / "finalize_summary.json"),
        dropped = str(Path(config['finalize_dir']) / "dropped_rows.csv") if config.get("export_drop_output", False) else None
    log:
         f"logs/finalize_{date_string}.log"
    params:
        drop_flag = "--drop-output" if config.get("export_drop_output", False) else "",
        cutoff: config["cutoff_value"]
    conda:
        "envs/nih.yml"
    shell:
        """
        python scripts/cli.py finalize \
            --keywords {input.keywords} \
            --config {input.config} \
            --output {output.finalize_dir} \
            --summary-json {output.summary} \
            --cutoff_value {params.cutoff} \
            {params.drop_flag} \
            > {log} 2>&1
        """
