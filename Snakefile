configfile: "config/config.yaml"

rule preprocess:
    input:
        "config/config.yaml"
    output:
        "results/cleaned.csv"
    log:
        "logs/preprocess.log"
    conda:
        "envs/nih.yml"
    shell:
        """
        python scripts/cli.py preprocess --output {output} > {log} 2>&1
        """

rule enrich_keywords:
    input:
        "results/cleaned.csv"
    output:
        "results/enriched.csv"
    log:
        "logs/enrich_keywords.log"
    conda:
        "envs/nih.yml"
    shell:
        """
        python scripts/cli.py enrich --output {output} > {log} 2>&1
        """

rule finalize_training:
    input:
        "results/enriched.csv"
    output:
        ["results/MLdf_training.csv", "results/MLdf_dropped.csv"]
    log:
        "logs/finalize_training.log"
    conda:
        "envs/nih.yml"
    shell:
        """
        python scripts/cli.py train > {log} 2>&1
        """
