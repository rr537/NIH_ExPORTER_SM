rule mlexport:
    input:
        config = "config/config.yaml",
        keywords = config["keywords_dir"]
    output:
        mlexport_dir = directory(config['mlexport_dir']),
        summary = str(Path(config['mlexport_dir']) / "mlexport_summary.json"),
        dropped = str(Path(config['mlexport_dir']) / "dropped_rows.csv") if config.get("export_drop_output", False) else None
    log:
        f"logs/mlexport_{date_string}.log"
    conda:
        config["env_path"]
    shell:
        """
        python bin/cli.py mlexport \
            --keywords {input.keywords} \
            --config {input.config} \
            --output {output.mlexport_dir} \
            --summary-json {output.summary} \
            > {log} 2>&1
        """
