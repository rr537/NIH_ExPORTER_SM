rule metrics:
    input:
        config = "config/config.yaml",
        pickles = config["preprocess_dir"]
    output:
        metrics_dir = directory(config['metrics_dir']),
        summary = str(Path(config['metrics_dir']) / "metrics_summary.json")
    log:
        f"logs/metrics_{date_string}.log"
    conda:
        config["env_path"]
    shell:
        """
        python bin/cli.py metrics \
            --pickles {input.pickles} \
            --config {input.config} \
            --output {output.metrics_dir} \
            --summary-json {output.summary} \
            > {log} 2>&1
        """
