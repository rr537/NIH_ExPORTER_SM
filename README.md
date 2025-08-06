# 🧬 NIH ExPORTER ML Pipeline

![MIT License](https://img.shields.io/badge/License-MIT-blue.svg)
![Python 3.9+](https://img.shields.io/badge/Python-3.9%2B-yellow)
![Snakemake Workflow](https://img.shields.io/badge/Workflow-Snakemake-purple)
![Issues](https://img.shields.io/github/issues/rr537/NIH_ExPORTER_SM)
![Last Commit](https://img.shields.io/github/last-commit/rr537/NIH_ExPORTER_SM)
![Stars](https://img.shields.io/github/stars/rr537/NIH_ExPORTER_SM?style=social)

A scalable and modular Snakemake workflow for processing NIH ExPORTER datasets into machine learning–ready training files—optimized for finding CRISPR/gene therapy related treatments for rare diseases but can be applied to other treatment-disease combinations, or used just to find relevant treatments are diseases. Features include preprocessing, keyword enrichment via FlashText, and configurable training data generation.

---

## Background

Analyzing historical NIH-funded research data often involves navigating:
- [NIH RePORTER](https://reporter.nih.gov/)
- [NIH ExPORTER](https://reporter.nih.gov/exporter/)
- [NIH RePORTER API](https://api.reporter.nih.gov/)

To overcome querying limits in both RePORTER and its API, this pipeline accelerates processing of complete datasets downloaded from NIH ExPORTER. This workflow helps identify relevant NIH studies by counting keyword occurrences and outputs a structured dataset suitable to train a machine learning classification model.

---

## Pipeline Overview

### Step 1: Data Loading and Data Preprocessing 

This stage loads raw NIH ExPORTER datasets and prepares them for downstream analysis. It is modular and fully configurable via `config.yaml`.

**1A. Configuration & Validation** 
- Loads pipeline configuration and sets logging level.
- Validates paths and checks for required data sources.

**1B. Data Ingestion** 
- Loads six NIH ExPORTER sources (CSV format):
- Tracks loading metadata for auditability.

| File Identifier   | Description           | Data Source Link |
|-------------------|-----------------------|------------------|
| `PRJ`             | 1. Projects           | [Projects](https://reporter.nih.gov/exporter/projects) |
| `PRJABS`          | 2. Abstracts          | [Abstracts](https://reporter.nih.gov/exporter/abstracts) |
| `PUB`             | 3. Publications       | [Publications](https://reporter.nih.gov/exporter/publications) |
| `Patents`         | 4. Patents            | [Patents](https://reporter.nih.gov/exporter/patents) |
| `ClinicalStudies` | 5. Clinical Studies   | [Clinical Studies](https://reporter.nih.gov/exporter/clinicalstudies) |
| `PUBLINK`         | 6. Linked Publications| [Link Tables](https://reporter.nih.gov/exporter/linktables) |

**1C. Column Renaming** 
- Resolves column name discrepancies using configurable mappings.
- Ensures consistency across datasets (e.g., `CORE_PROJECT_NUM` → `PROJECT_NUMBER`).

**1D. Appending by Source** 
- Consolidates year-based files (e.g., `RePORTER_PRJ_C_FY2024`, `RePORTER_PRJ_C_FY2023`, `RePORTER_PRJ_C_FY2022`) into unified datasets per source (e.g., `PRJ`).
- Outputs are saved as `.pkl` files for efficient downstream access.

**1E. Metadata Assembly**
- Generates metadata summaries for ingestion, renaming, and appending steps.
- Combines into a unified preprocessing summary.

**1F. Summary Export**
- Exports preprocessing summary as a JSON file (`preprocessing_summary.json`).
- Includes row counts, column mappings, and source-level statistics.

### Step 2: Project Metrics & Deduplication

This stage links datasets, aggregates project outcomes, and removes duplicate records. It builds a metrics-rich dataset for downstream keyword analysis and ML export.

**2A. Data Input Resolution**
- Resolves input pickle files from the preprocessing stage.
- Loads all DataFrames into memory for processing.

**2B. Linking Records**
- Merges linked datasets (e.g., Projects with Abstracts) using shared identifiers (e.g., `APPLICATION_ID`).
- Tracks linkage metadata for auditability.

**2C. Aggregating Outcomes**
- Counts publications (`PMID`), patents (`PATENT_ID`), and clinical studies (`ClinicalTrials.gov ID`) per `PROJECT_NUMBER`.
- Produces a unified metrics DataFrame with normalized columns (trim whitespace, uppercase)

**2D. Deduplication**
- Removes true duplicate rows via full-record comparison.
- Updates deduplication summary with:
  - Unique duplicates
  - Total duplicates
  - Extra duplicates beyond first occurrence

**2E. Export & Summary**
- Exports final metrics dataset to CSV.
- Generates a comprehensive JSON summary (`metrics_summary.json`) with:
  - Linkage stats
  - Aggregation counts
  - Deduplication metrics

## Step 3: Keyword Enrichment & Metadata

This stage enriches the metrics dataset with keyword-level insights, including disease and treatment tagging. It produces a keyword-annotated dataset and a detailed summary for downstream analysis.

**3A. Data Input Resolution**
- Loads the metrics dataset from the previous stage.
- Resolves input paths and validates schema.

**3B. Keyword Preparation**
- Generates keyword variants from base keywords defined in `config.yaml` (`treatment`, `disease`) using rules for pluralization, possessives, hyphens,
- Applies optional stopword filtering based on config.
- Note: For the current pipeline, keywords in either `treatment` or `disease` categories will be combined and treated the same way. 

**3C. Enrichment**
- Text columns (`PROJECT_TITLE`, `PROJECT_TERMS`, `PHR`, `ABSTRACT_TEXT`) are concatenated.
- Keywords and variants are matched and tagged using FlashText. 
- Computes enrichment metrics:
  - Keyword frequency
  - Co-occurrence patterns
  - Coverage across records

**3D. Export & Summary**
- Exports keyword-enriched dataset to CSV.
- Generates a JSON summary (`keywords_summary.json`) with:
  - Keywords and generated variants 
  - Treatment and disease tagging stats
  - Matching coverage
  - Stopword usage (if enabled)

## Step 4: ML Export

This stage filters the keyword-enriched dataset to produce a machine learning–ready output. It applies configurable rules to select relevant records and exports both the final and dropped rows.

**4A. Data Input Resolution**
- Loads the keyword-enriched dataset from the previous stage.
- Resolves input paths and validates schema.

**4B. Filtering Logic**
- Applies configurable filters (e.g., `total_unique_count >= 1`) to select relevant records.
- Selected columns (defined in `config.yaml` under `ml_columns`) form the final ML-ready dataset.
- Drops rows that do not meet criteria and optionally exports them.

**4C. Export & Summary**
- Exports ML-ready dataset to CSV.
- Optionally exports dropped rows (`dropped_rows.csv`) if enabled in config.
- Generates a JSON summary (`finalize_summary.json`) with:
  - Filter criteria
  - Row counts (kept vs dropped)
  - Column-level stats

---

## 🚀 Features

- **Modular Rule Design**  
  Clean separation of logic across `preprocess`, `metrics`, `keywords`, and `mlexport` stages for maintainability and clarity.

- **Config-Driven Execution**  
  Centralized `config.yaml` controls paths, filters, keyword lists, logging levels, and export options—no hardcoded values.

- **Robust Deduplication**  
  Full-record comparison with detailed tracking of unique, total, and excess duplicates. Outputs audit-ready summaries.

- **Keyword Enrichment with FlashText**  
  Fast keyword matching with support for pluralization, punctuation, and stopword filtering.

- **Parallel & Batch Processing**  
  Optimized for scalability across large datasets using concurrent processing strategies.

- **Audit-Ready Summaries**  
  Each stage exports structured JSON summaries with row counts, metadata, and transformation stats.

- **Snakemake Automation**  
  Fully reproducible workflow with conda environment support, logging, and rule-level modularity.

- **ML-Ready Dataset Export**  
  Final output is filtered and curated for downstream machine learning tasks, with optional dropped-row tracking.

- **Environment Portability**  
  Uses absolute path resolution and centralized environment management (`envs/nih.yml`) for cross-platform compatibility.

- **Clear Logging & Diagnostics**  
  Stage-specific logs with configurable verbosity for debugging and traceability.

---

## 📁 Directory Structure

```text
project-root/
├── workflows/
│   ├── Snakefile                    # Entry point for Snakemake
│   └── rules/                       # Modular rule files
│       ├── common.smk               # Shared utilities and functions
│       ├── preprocess.smk           # Data ingestion, renaming, and appending
│       ├── metrics.smk              # Linking, aggregation, and deduplication
│       ├── keywords.smk             # Keyword tagging and enrichment
│       └── mlexport.smk             # ML dataset filtering and export
├── config/
│   └── config.yaml                  # Centralized pipeline configuration
├── envs/
│   └── nih.yml                      # Conda environment for reproducibility
├── logs/
│   └── *.log                        # Stage-specific logs with timestamps
├── data/
│   └── raw/                         # Source NIH ExPORTER datasets
│       ├── PRJ/
│       ├── PRJABS/
│       ├── PUB/
│       ├── Patents/
│       ├── ClinicalStudies/
│       └── PUBLINK/
├── results/
│   ├── preprocess/                 # Pickled and summary outputs from preprocessing
│   ├── metrics/                    # Aggregated metrics and deduplication outputs
│   ├── keywords/                   # Keyword-enriched datasets and summaries
│   └── mlexport/                   # Final ML-ready dataset and dropped rows
├── bin/
│   └── cli.py                      # CLI entry point for each pipeline stage
├── src/
│   └── common/
│       ├── config_loader.py  #
│       ├── logger.py
│       └── path_utils.py
│   └── preprocess/
│       ├── preprocess_pipeline.py  
│       ├── preprocess_validator.py 
│       ├── preprocess_io.py
│       ├── preprocess_transform.py
│       └── preprocess_summary.py
│   └── metrics/
│       ├── metrics_pipeline.py  
│       ├── metrics_io.py  
│       ├── metrics_merge.py  
│       ├── metrics_aggregate.py  
│       ├── metrics_dedupe.py  
│       └── metrics_summary.py  
│   └── keywords/
│       ├── keywords_pipeline.py  
│       ├── keywords_io.py  
│       ├── keywords_generator.py  
│       └── keywords_enrichment.py  
│   └── mlexport/
│       ├── mlexport_pipeline.py  
│       ├── mlexport_io.py  
│       ├── mlexport_transform.py  
│       └── mlexport_summary.py  
├── LICENSE.md
└── README.md

---

##  Usage

## 📦 Dependencies

This project uses the `nih_env` Conda environment for reproducible data processing and ML pipeline execution.

Environment configuration (`environment.yml`):

- **Channels**
  - `conda-forge`
  - `defaults`

- **Dependencies**
  - `python=3.11`
  - `pandas=2.3.0`
  - `numpy=2.3.0`
  - `pyarrow=20.0.0`
  - `flashtext=2.7`
  - `nltk=3.9.1`
  - `unidecode` (latest)
  - `tqdm=4.67.1`
  - `pyyaml`

### 🔧 Setup

To recreate this environment:

```bash
# Create the environment
conda env create -f envs/nih.yml

# Activate the environment
conda activate nih_env
```

### 🐍 Run Workflow 
```bash
# Full pipeline (Note: finalize_training is a placeholder rule and may require adjustment.)
snakemake \
  --snakefile workflows/Snakefile \   # Path to the Snakefile
  --configfile config/config.yaml \   # Path to config.yaml file 
  --use-conda \                       # Use conda environments as specified
  --cores 4 \                         # Specify number of CPU cores
  finalize                            # Target rule
\



# Manual CLI execution
# Preprocessing step
python bin/cli.py preprocess --config config/config.yaml \
                             --output results/cleaned.csv \
                             --summary-json results/preprocessing_summary.json

# Metrics
python bin/cli.py metrics --config config/config.yaml \

# Keywords
python bin/cli.py keywords --config config/config.yaml \

# Finalize
python bin/cli.py mlexport --config config/config.yaml \
```

### 📤 Pipeline Outputs

Upon successful execution, the pipeline will generate the following key outputs:

- `results/MLdf_training.csv`  
  → Final ML-ready dataset containing rows with matched keywords

- `results/MLdf_dropped.csv`  
  → Dataset of rows excluded due to absence of keyword matches

- `results/dedup_summary.csv`  
  → Summary of duplicate detection and resolution across datasets

- `results/summary.json`  
  → Logs and metrics from preprocessing steps
---

## 🧠 Keyword Strategy

Keywords are enriched using FlashText with rules for:
- Pluralization and possessives  
- Hyphen and accent normalization  
- Optional stopword filtering

Note: There is no functional distinction between entries listed under `treatment` or `disease` in `config.yaml`; both are used for enrichment and matching.

---

## 🔑 Configurable Parameters (`config/config.yaml`)

| Key                        | Description |
|---------------------------|-------------|
| `folder`                  | Root directory containing all ExPORTER subfolders  
| `subfolders`              | Names of subdirectories with source datasets  
| `rename_columns_map`      | Dictionary mapping old column names to standardized names  
| `treatment`, `disease`    | Lists of base keywords for enrichment  
| `text_columns`            | Target columns for keyword matching  
| `ml_columns`              | Columns to include in final ML-ready export  
| `output_dir`              | Output destination for results  
| `dedup_summary_csv_path`  | Path for deduplication summary output  
| `parallel`                | Enable/disable multiprocessing  
| `workers`                 | Number of parallel threads for processing  
| `loglevel`                | Logging verbosity level  
| `remove_duplicates`       | Enable/disable deduplication logic  

---

## 📘 Data Dictionary (`MLdf_training.csv`)

| Column              | Description                                      |
|---------------------|--------------------------------------------------|
| APPLICATION_ID      | Unique funding application ID                    |
| PHR                 | Public Health Relevance Statement                |
| PROJECT_TERMS       | NIH-defined project descriptors                  |
| PROJECT_TITLE       | Project title                                    |
| ABSTRACT_TEXT       | Project abstract                                 |
| total count         | Total matched keywords (treatment + disease)     |
| total unique count  | Count of unique keyword matches                  |
| flagged             | List of matched keyword terms per row            |

---

## Citation 

**APA Style (7th edition)**<br>
Racharaks, R. (2025). NIH Keyword-Enrichment and ML Training Pipeline. GitHub. https://github.com/rr537/NIH_ExPORTER_SM

---