# NIH_ExPORTER_SM
NIH ExPORTER ML Pipeline

README: NIH ExPORTER ML Pipeline

This repository contains a scalable, modular data pipeline for processing NIH ExPORTER data into machine learning–ready datasets. It includes preprocessing, keyword enrichment using FlashText, and training data extraction for rare disease–focused modeling.

Features:
- Config-driven folder & column management
- Keyword tagging using FlashText with enrichment & stopword control
- Deduplication logic for robust cleaning
- Parallel processing support for large datasets
- Snakemake-powered reproducibility

Directory Structure:
project-root/
├── config/
│   └── config.yaml
├── envs/
│   └── nih.yml
├── logs/
├── results/
├── scripts/
│   ├── cli.py
│   ├── pipeline.py
│   ├── config_loader.py
│   ├── preprocessing.py
│   ├── enrichment.py
│   ├── keywords.py
│   ├── merge.py
│   ├── aggregation.py
│   ├── training.py
│   ├── loader.py
│   └── logger.py
└── Snakefile

Usage:
# Setup environment
conda env create -f envs/nih.yml
conda activate nih_env

# Run full pipeline
snakemake --use-conda --cores 4 finalize_training

# Or run steps manually:
python scripts/cli.py preprocess --config config/config.yaml --output results/cleaned.csv
python scripts/cli.py enrich --config config/config.yaml --output results/enriched.csv
python scripts/cli.py train --config config/config.yaml --stopwords

ML Output:
- results/MLdf_training.csv → ML-ready dataset
- results/MLdf_dropped.csv → Rows without keyword matches

Keyword Strategy:
FlashText enriches terms using pluralization, punctuation normalization, and accent removal. Config supports omit lists and stopword control.

Configurable Keys (in config/config.yaml):
- folder, subfolders
- drop_col_header_map, rename_columns_map
- keywords → treatment, disease, omit
- text_columns, ml_columns
- output_dir, parallel, loglevel, remove_duplicates

---

DATA DICTIONARY: MLdf_training.csv

APPLICATION_ID       → Unique funding application identifier  
PHR                  → Public health relevance statement  
PROJECT_TERMS        → NIH-defined project descriptors  
PROJECT_TITLE        → Title of the funded project  
ABSTRACT_TEXT        → Project abstract  
total count          → Total keyword matches (treatment + disease)  
total unique count   → Unique keyword matches  
flagged              → List of matched keyword terms per row  