rule keywords:
    input:
        config = "config/config.yaml",
        metrics = config["metrics_dir"]
    output:
        keywords_dir = directory(config['keywords_dir']),
        summary = str(Path(config['keywords_dir']) / "keywords_summary.json")
    log:
        f"logs/keywords_{date_string}{'_stopwords' if config.get('use_stopwords', False) else ''}.log"
    conda:
        config["env_path"]
    shell:
        """
        python bin/cli.py keywords \
            --metrics {input.metrics} \
            --config {input.config} \
            --output {output.keywords_dir} \
            --summary-json {output.summary} \
            > {log} 2>&1
        """
