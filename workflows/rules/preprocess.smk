rule preprocess:
    input:
        config = "config/config.yaml"
    output:
        preprocess_dir = directory(config["preprocess_dir"])
    params:
        summary = str(Path(config['preprocess_dir']) / "preprocess_summary.json")
    log:
        f"logs/preprocess_{date_string}.log"
    conda:
        config["env_path"]
    shell:
        """
        python bin/cli.py preprocess \
            --config {input.config} \
            --output {output.preprocess_dir} \
            --summary-json {params.summary} \
            > {log} 2>&1
        """