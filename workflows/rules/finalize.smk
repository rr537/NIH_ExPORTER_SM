rule finalize:
    input:
        config = "config/config.yaml",
        keywords = config["keywords_dir"]
    output:
        finalize_dir = directory(config['finalize_dir']),
        summary = str(Path(config['finalize_dir']) / "finalize_summary.json"),
        dropped = str(Path(config['finalize_dir']) / "dropped_rows.csv") if config.get("export_drop_output", False) else None
    log:
        f"logs/finalize_{date_string}.log"
    conda:
        config["env_path"]
    shell:
        """
        python bin/cli.py finalize \
            --keywords {input.keywords} \
            --config {input.config} \
            --output {output.finalize_dir} \
            --summary-json {output.summary} \
            > {log} 2>&1
        """
