# 🧬 NIH ExPORTER ML Pipeline

![MIT License](https://img.shields.io/badge/License-MIT-blue.svg)
![Python 3.9+](https://img.shields.io/badge/Python-3.9%2B-yellow)
![Snakemake Workflow](https://img.shields.io/badge/Workflow-Snakemake-purple)
![Issues](https://img.shields.io/github/issues/rr537/NIH_ExPORTER_SM)
![Last Commit](https://img.shields.io/github/last-commit/rr537/NIH_ExPORTER_SM)
![Stars](https://img.shields.io/github/stars/rr537/NIH_ExPORTER_SM?style=social)

A scalable and modular **Snakemake workflow** for transforming NIH ExPORTER datasets into machine learning–ready training files. Designed to identify **CRISPR and gene therapy–related treatments for rare diseases**, the pipeline is fully configurable and adaptable to other treatment-disease combinations.

### ✨ Core Features
- **Modular preprocessing** of NIH datasets with audit-ready summaries
- **Keyword enrichment** using FlashText with support for pluralization, stopword filtering, and accent normalization
- **Config-driven ML export** with customizable filters and column selection
- **Parallel processing** and conda-based reproducibility

Whether you're exploring biomedical trends or exploring NIH funding history, this pipeline offers a robust foundation for scalable, transparent analysis.

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

### Step 3: Keyword Enrichment & Metadata

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

### Step 4: ML Export

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
│   └── common/                     # Shared utilities used across all pipeline stages
│       ├── __init__.py             # Initializes module
│       ├── config_loader.py        # Loads and parses config.yaml; supports nested keys and defaults
│       ├── logger.py               # Centralized logging setup with stage-specific loggers
│       └── path_utils.py           # Path resolution, directory creation, and cross-platform support
│
│   └── preprocess/                 # Handles raw data ingestion and transformation
│       ├── __init__.py             # Initializes module
│       ├── preprocess_pipeline.py  # Orchestrates preprocessing steps; entry point for Snakemake rule
│       ├── preprocess_validator.py # Validates input formats, schema consistency, and required fields
│       ├── preprocess_io.py        # Reads and writes raw/preprocessed data; handles pickling
│       ├── preprocess_transform.py # Renaming, appending, and structural transformations
│       └── preprocess_summary.py   # Generates summary stats and audit logs for preprocessing
│ 
│   └── metrics/                    # Aggregates and deduplicates project-level metrics
│       ├── __init__.py             # Initializes module
│       ├── metrics_pipeline.py     # Main pipeline logic for metrics stage
│       ├── metrics_io.py           # I/O functions for metrics datasets and intermediate files
│       ├── metrics_merge.py        # Merges datasets across sources (e.g., PRJ, PUB)
│       ├── metrics_aggregate.py    # Computes aggregates (e.g., funding totals, publication counts)
│       ├── metrics_dedupe.py       # Deduplication logic
│       └── metrics_summary.py      # Summary outputs and diagnostics for metrics stage
│ 
│   └── keywords/                   # Adds keyword annotations and enrichment
│       ├── __init__.py             # Initializes module
│       ├── keywords_pipeline.py    # Entry point for keyword tagging and enrichment
│       ├── keywords_io.py          # Handles reading/writing keyword datasets
│       ├── keywords_generator.py   # Generates keyword tags 
│       └── keywords_enrichment.py  # Enriches datasets with keyword metadata 
│ 
│   └── mlexport/                   # Final ML dataset preparation and export
│       ├── __init__.py             # Initializes module
│       ├── mlexport_pipeline.py    # Coordinates filtering and export steps
│       ├── mlexport_io.py          # I/O for ML-ready datasets and dropped rows
│       ├── mlexport_transform.py   # Applies final filters and feature selection
│       └── mlexport_summary.py     # Summarizes export results and logs dropped rows
├── LICENSE.md
└── README.md
```

##  Usage

### 📦 Dependencies

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

# Check installed packages
conda list
```

## 🐍 Run Workflow 
### To execute the full pipeline:
```bash
snakemake --snakefile workflows/Snakefile --configfile config/config.yaml --use-conda --cores 4 
mlexport                           
```
Code description 
```bash
snakemake \
  --snakefile workflows/Snakefile \     # Path to Snakefile
  --configfile config/config.yaml \     # Pipeline configuration
  --use-conda \                         # Enable Conda environments
  --cores 4 \                           # Number of CPU cores
  mlexport                              # Target rule
```
Note: mlexport is the final rule. You can substitute it with any intermediate rule (e.g., preprocess, keywords) for partial runs.

### To preview the workflow without executing:
```bash
snakemake \
  --snakefile workflows/Snakefile \
  --configfile config/config.yaml \
  --use-conda \
  --cores 4 \
  --dry-run
```
## Manual CLI execution

You can run individual pipeline stages manually using the CLI tool:

### Preprocess
```bash
python bin/cli.py preprocess --config config/config.yaml \
```
- Inital data loading and preprocessing of NIH ExPORTER files
- Outputs preprocessed pickle files and a JSON summary

### Metrics
```bash
python bin/cli.py metrics --config config/config.yaml \
```
- Aggregates and deduplicates project-level metrics
- Outputs metrics CSV and a JSON summary 

### Keywords
```bash
python bin/cli.py keywords --config config/config.yaml \
```
- Extracts domain-specific keywords using FlashText
- Outputs enriched datasets with keyword metadata and a JSON summary 

### Mlexport
```bash
python bin/cli.py mlexport --config config/config.yaml \
```
- Applies final filters and feature selection
- Outputs final ML dataset, optional dropped rows dataset, and a JSON summary

## 📤 Pipeline Outputs

Upon successful execution, the pipeline will generate the following key outputs, organized by stage:

###  Preprocessing (`results/preprocess/`)
- `*.pkl` files  
  → Serialized datasets for each source (e.g., Projects, Abstracts)

- `preprocess_summary.json`  
  → Summary of ingestion, renaming, and appending steps

---

###  Metrics (`results/metrics/`)
- `metrics.csv`  
  → Aggregated project-level metrics including publications, patents, and clinical studies

- `metrics_summary.json`  
  → Summary of linkage, aggregation, and deduplication statistics

---

###  Keywords (`results/keywords/`)
- `keywords.csv`  
  → Keyword-enriched dataset with tagging and frequency metrics

- `keywords_summary.json`  
  → Summary of keyword generation, enrichment coverage, and stopword usage

---

###  ML Export (`results/mlexport/`)
- `mlexport.csv`  
  → Final ML-ready dataset containing rows with matched keywords

- `dropped_rows.csv` *(optional)*  
  → Rows excluded due to absence of keyword matches (if `export_drop_output` is enabled)

- `mlexport_summary.json`  
  → Summary of filtering logic, row counts, and export diagnostics
---

## 🧠 Keyword Strategy

Keyword enrichment is performed using **FlashText**, optimized for speed and precision across large datasets. The strategy includes:

### Matching Rules
- **Pluralization & Possessives**  
  → Handles variations like “therapy” vs. “therapies” or “patient’s” vs. “patients”

- **Hyphen & Accent Normalization**  
  → Converts terms like “T-cell” to “T cell” and removes diacritics for consistent matching

- **Case-Insensitive Matching**  
  → Ensures keywords are detected regardless of capitalization

- **Optional Stopword Filtering**  
  → Removes common non-informative words to reduce false positives (configurable)

---

### Keyword Sources
- Keywords are loaded from `config.yaml` under both `treatment` and `disease` categories  
  → These categories are **logically grouped** but treated identically during enrichment

- All keywords are preprocessed and deduplicated before matching

---

### Enrichment Output
- Each matched keyword is tagged with its source category and frequency  
- Enrichment coverage is logged in `keywords_summary.json`

---

## 🔑 Configurable Parameters (`config/config.yaml`)

The pipeline is governed by a modular configuration file that defines input sources, enrichment logic, execution settings, and output destinations.

### 📁 Input & Preprocessing
| Key                  | Description |
|----------------------|-------------|
| `folder`             | Root directory containing all ExPORTER subfolders |
| `subfolders`         | List of dataset-specific subdirectories to ingest |
| `rename_columns_map` | Maps inconsistent column names to standardized schema |
| `force_append`       | Enables forced appending of datasets across folders |

---

### 🧠 Keyword Enrichment
| Key                        | Description |
|----------------------------|-------------|
| `keywords.treatment`       | List of treatment-related keywords for enrichment |
| `keywords.disease`         | List of disease-related keywords for enrichment |
| `keywords.remove_stopwords`| Enables stopword filtering during keyword matching |
| `text_columns`             | Target columns for keyword scanning |
| `cutoff_value`             | Minimum keyword match count required for ML inclusion |

---

### 📦 ML Export
| Key                  | Description |
|----------------------|-------------|
| `ml_columns`         | Columns to include in final ML-ready dataset |
| `export_drop_output`| Enables export of dropped rows with no keyword matches |

---

### ⚙️ Execution Settings
| Key            | Description |
|----------------|-------------|
| `parallel`     | Enables multiprocessing for faster execution |
| `workers`      | Number of parallel threads to use |
| `loglevel`     | Logging verbosity level (`DEBUG`, `INFO`, etc.) |
| `logs_dir`     | Directory for log file storage |
| `log_to_file`  | Enables logging to file |
| `log_to_console`| Enables logging to console |

---

### 📂 Output Paths
| Key                  | Description |
|----------------------|-------------|
| `output_dir`         | Root directory for all pipeline outputs |
| `preprocess_dir`     | Directory for serialized preprocessed datasets |
| `metrics_dir`        | Directory for aggregated metrics and summaries |
| `keywords_dir`       | Directory for keyword-enriched outputs |
| `mlexport_dir`       | Directory for ML-ready exports |

---

## 📘 Data Dictionary (`mlexport.csv`)

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