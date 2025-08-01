from datetime import datetime
date_string = datetime.today().strftime("%Y-%m-%d")

configfile: "config/config.yaml"

rule preprocess:
    input:
        "config/config.yaml"
    output:
        pkl_dir=directory("results/preprocessed/"),
        summary="results/preprocess_summary.json"
    log:
        f"logs/preprocess_{date_string}.log"
    conda:
        "envs/nih.yml"
    shell:
        """
        python scripts/cli.py preprocess --config {input.config} --output {output.pkl_dir} --summary-json {output.summary} > {log} 2>&1
        """

rule enrich_keywords:
    input:
        cleaned="results/cleaned.csv",
        config="config/config.yaml"
    output:
        enriched="results/enriched.csv"
    log:
        f"logs/enrich_keywords_{date_string}.log"
    conda:
        "envs/nih.yml"
    shell:
        """
        python scripts/cli.py enrich --config {input.config} --output {output} --log-file {log} > {log} 2>&1
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

rule export_appended_dict:
    input:
        script="scripts/cli.py",
        config="config/config.yaml"
    output:
        expand("validation_output/appended_dict/{name}.pkl", name=[
            "ClinicalStudies", "Patents", "PRJ", "PRJABS", "PUB", "PUBLINK"
        ])
    params:
        export_dir="validation_output/appended_dict"
    shell:
        """
        python {input.script} export_dict \
        --config {input.config} \
        --export_dir {params.export_dir}
        """

rule summarize_pipeline:
    input:
        preprocess_log="results/preprocess_metadata.json",
        enrich_log="results/enrich_metadata.json"
    output:
        summary_json="results/pipeline_summary.json"
    run:
        import json
        from build_summary import build_summary

        with open(input.preprocess_log) as f1:
            pre_stats = json.load(f1)
        with open(input.enrich_log) as f2:
            enrich_stats = json.load(f2)

        summary = build_summary(pre_stats, enrich_stats)
        with open(output.summary_json, "w") as fout:
            json.dump(summary, fout, indent=2)
