# 🧬 NIH ExPORTER ML Pipeline

![MIT License](https://img.shields.io/badge/License-MIT-blue.svg)
![Python 3.9+](https://img.shields.io/badge/Python-3.9%2B-yellow)
![Snakemake Workflow](https://img.shields.io/badge/Workflow-Snakemake-purple)
![Issues](https://img.shields.io/github/issues/rr537/NIH_ExPORTER_SM)
![Last Commit](https://img.shields.io/github/last-commit/rr537/NIH_ExPORTER_SM)
![Stars](https://img.shields.io/github/stars/rr537/NIH_ExPORTER_SM?style=social)

A scalable and modular Snakemake workflow for processing NIH ExPORTER datasets into machine learning–ready training files—optimized for finding CRISPR/gene therapy related treatments for rare diseases. Features include preprocessing, keyword enrichment via FlashText, and configurable training data generation.

---

## Background

Analyzing historical NIH-funded research data often involves navigating:
- [NIH RePORTER](https://reporter.nih.gov/)
- [NIH ExPORTER](https://reporter.nih.gov/exporter/)
- [NIH RePORTER API](https://api.reporter.nih.gov/)

To overcome querying limits in both RePORTER and its API, this pipeline accelerates processing of complete datasets downloaded from NIH ExPORTER. This workflow helps identify relevant NIH studies by counting keyword occurrences and outputs a structured dataset suitable to train a machine learning classification model.

---

## Pipeline Overview

### Step 1: Data Loading
Ingests 6 NIH ExPORTER sources (CSV format):
1. Projects  
2. Abstracts  
3. Publications  
4. Patents  
5. Clinical Studies  
6. Linked Publications

### Step 2: Data Preprocessing

- **2A. Renaming**  
  Configurable rules (`config.yaml`) to resolve column name discrepancies which have been observed in the NIH ExPORTER datasets (e.g., `CORE_PROJECT_NUM`, `PROJECT_ID`, `Core Project Number` → `PROJECT_NUMBER`).

- **2B. Appending**  
  Consolidates year-based files (e.g., `RePORTER_PRJ_C_FY2024`, `~FY2023`, `~FY2022`) into a single dataset by source.

- **2C. Linking**  
  Merges Projects and Abstracts datasets using `APPLICATION_ID`.

- **2D. Aggregating Outcomes**  
  Counts publications (`PMID`), patents (`PATENT_ID`), and clinical studies (`ClinicalTrials.gov ID`) per `PROJECT_NUMBER`. Normalizes key columns (trim whitespace, uppercase) and tracks:
  - Unique duplicate rows  
  - Total duplicates  
  - Extra duplicates beyond first occurrence  

  Outputs results to `dedup_summary.csv` and merges back into the linked dataset.

- **2E. Deduplication**  
  Deduplicates the merged dataset by full row comparison. Updates the dedup summary table.

### Step 3: Keyword Identification & Counting

- **3A. Variant Generation (Enrichment)**  
  Generates keyword variants from base keywords defined in `config.yaml` (`treatment`, `disease`) using rules for pluralization, possessives, hyphens, and stopword filtering.

  Note: For the current pipeline, it does not matter what kind of keyword is added to either the `treatment` or `disease` categories. Keywords in each category will be combined and treated the same way. 

- **3B. Matching & Counting**  
  Text columns (`PROJECT_TITLE`, `PROJECT_TERMS`, `PHR`, `ABSTRACT_TEXT`) are concatenated. Keywords and variants are matched using FlashText. Supports batch and parallel processing.

### Step 4: ML Training Dataset Export  
Filters rows with `total_unique_count > 0`. Selected columns (defined in `config.yaml` under `ml_columns`) form the final ML-ready dataset.

---

## 🚀 Features
-  **Config-driven folder & column mapping**
-  **Deduplication logic** for clean, reliable outputs
-  **Keyword tagging** with FlashText: enrichment, stopword control, plural/punctuation/accent handling
-  **Parallel processing support** for scalability
-  **Snakemake automation** for reproducibility

---

## 📂 Directory Structure

```text
project-root/
├── config/
│   └── config.yaml
├── envs/
│   └── nih.yml
├── logs/
├── data/
│   └── raw/
│       ├── Projects/
│       ├── Abstracts/
│       ├── Publications/
│       ├── Patents/
│       ├── Clinical Studies/
│       └── Linked Publications/
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
├── Snakefile
├── LICENSE.md
└── README.md
```

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
snakemake --use-conda --cores 4 finalize_training

# Manual CLI execution
# Preprocessing step
python scripts/cli.py preprocess --config config/config.yaml \
                                 --output results/cleaned.csv \
                                 --summary-json results/preprocessing_summary.json

# Keyword enrichment
python scripts/cli.py enrich --config config/config.yaml \
                             --output results/enriched.csv

# ML training dataset creation
python scripts/cli.py train --config config/config.yaml --stopwords
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

APA Style (7th edition)
Racharaks, R. (2025). NIH Keyword-Enrichment and ML Training Pipeline. GitHub. https://github.com/rr537/NIH_ExPORTER_SM

---