configfile: "config/config.yaml"

rule preprocess:
    input:
        "config/config.yaml"
    output:
        "results/cleaned.csv"
    log:
        f"logs/preprocess_{date_string}.log"
    conda:
        "envs/nih.yml"
    shell:
        """
        python scripts/cli.py preprocess --output {output} --log-file {log} > {log} 2>&1
        """

rule enrich_keywords:
    input:
        "results/cleaned.csv"
    output:
        "results/enriched.csv"
    log:
        f"logs/enrich_keywords_{date_string}.log"
    conda:
        "envs/nih.yml"
    shell:
        """
        python scripts/cli.py enrich --output {output} --log-file {log} > {log} 2>&1
        """

rule finalize_training:
    input:
        enriched="results/enriched.csv",
        config="config/config.yaml"
    output:
        training="results/MLdf_training.csv",
        dropped="results/MLdf_dropped.csv",
        summary="results/summary.json"
    log:
         f"logs/finalize_training_{date_string}.log"
    conda:
        "envs/nih.yml"
    shell:
        """
        python scripts/cli.py train --config {input.config} --stopwords --log-file {log} > {log} 2>&1
        """
