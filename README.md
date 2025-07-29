# 🧬 NIH ExPORTER ML Pipeline

A scalable and modular Snakemake workflow for processing NIH ExPORTER datasets into machine learning–ready training files—optimized for rare disease modeling. Features include robust preprocessing, keyword enrichment via FlashText, and configurable training data generation.

---

## Background

Analyzing historical NIH data for funded scientific projects is typically labor intensive and involves pulling data from NIH RePORTER (https://reporter.nih.gov/), NIH ExPORTER (https://reporter.nih.gov/exporter/), or calling the NIH RePORTER API (https://api.reporter.nih.gov/) directly. As both NIH RePORTER and the API have query limitations, this SnakeMake workflow was designed to expedite the analysis of complete datasets which can be downloaded directly from NIH ExPORTER. In particular, this workflow helps identify relevant NIH studies based on the counts of unique and total preselected keywords and creates a subsequent dataset that can be used to train a ML classification model. 

---

## Pipeline Overview
Step 1: Data Loading 

In its current form, this pipeline ingests 6 sources of (.csv) data downloaded directly from NIH ExPORTER: 
    1) Projects
    2) Abstracts
    3) Publications
    4) Patents
    5) Clinical Studies

Step 2: Data Preproccessing

2A. Renaming:
    Renaming rules can be defined in the config.yaml. These renaming rules can be used to correct for column name descrepencies which have been observed in the NIH ExPORTER datasets. 

    For example: 'CORE_PROJECT_NUM', 'PROJECT_ID', and 'Core Project Number' have been observed to be used interchangable to identify the 'Project Number' associated with a given project. 

2B. Appending:
    This step appends (.csv) datasets based on their source. For example, if Projects (.csv) files from 2024-2022 were being analyzed, the append step would consolidate 'RePORTER_PRJ_C_FY2024', 'RePORTER_PRJ_C_FY2023', RePORTER_PRJ_C_FY2022' into a single dataset.

2C. Linking:
    This step creates a merged dataset of the Projects and Abstracts (.csv) datasets based on the shared 'APPLICATION_ID'.

2D. Aggregating Project Outcomes:
    This step counts the number of publications, patents, and clinical studies for a given funded project defined by a'PROJECT_NUMBER'. To do so, 'PROJECT_NUMBER' is paired with with a unique outcome identifier. For example, for publication counts, 'PROJECT_NUMBER' is paired with "PMID". These columns are then normalalized by 1) removing extra whitespaces and 2) uppercasing all values. After normalization, the count of unique duplicate rows ('unique_duplicate_rows'), the count of all duplicate rows ('total_duplicates'), and count of extra duplicates beyond the first occurrence ('extra_duplicates') are calculated with a summary table of all duplicates outputed to dedup_summary.csv. 

    Project outcome counts are then merged to the linked dataset generated in 2C.

2E. Deduplication:
    The merged dataset generated in 2D is deduplicated based on the entire row with summary counts added to the dedup_summary.csv. 

Step 3. Keyword identification and counts

3A. Keyword varient generation (enrichment)
    Keywords are defined in the config.yaml under 'Treatments' or 'Diseases'. Keyword varients are then generated using basic rules based on pluralization, possessive forms, and hyphens. An optional filter to remove stopwords from keywords can also be used. 
    
    Note: For the current pipeline, it does not matter what keyword is added into either the 'Treatments' or 'Diseases' categorys. 

3B. Keyword matching and counts
    For efficient keyword matching, columns with releveant project text data are then identified and combined into a single text field. For example, the current pipeline combines 'PROJECT_TITLE', 'PROJECT_TERMS', 'PHR', and 'ABSTRACT_TEXT'. Original keywords and generated varients are then matched exactly using FlashText. Optional parallel and batch processing can be configured to optimize processing speeds. 

Step 4. Preliminary ML training dataset generation
    A ML training-ready dataset is then created using rows with unique keyword matches > 0 ('total unique count' > 0) and preselect columns defined in the config.yaml under 'ml_columns'. 

---

## 🚀 Features
-  **Config-driven folder & column mapping**
-  **Deduplication logic** for clean, reliable outputs
-  **Keyword tagging** with FlashText: enrichment, stopword control, plural/punctuation/accent handling
-  **Parallel processing** for large-scale datasets
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
```

---

##  Usage

### 🔧 Setup
```bash
conda env create -f envs/nih.yml
conda activate nih_env
```

### 🐍 Run Workflow 
```bash
# Full pipeline (WIP)
snakemake --use-conda --cores 4 finalize_training

# Manual CLI execution
python scripts/cli.py preprocess --config config/config.yaml \
                                 --output results/cleaned.csv \
                                 --summary-json results/preprocessing_summary.json

python scripts/cli.py enrich --config config/config.yaml \
                             --output results/enriched.csv

python scripts/cli.py train --config config/config.yaml --stopwords
```

### 📤 Pipeline Outputs
- `results/MLdf_training.csv` → ML-ready dataset  
- `results/MLdf_dropped.csv` → Rows without keyword matches
- `results/dedup_summary.csv` → Deduplication summary table 
- `results/summary.json` → Preprocessing log  

---

## 🧠 Keyword Strategy

FlashText enriches terms with:
- Plural/punctuation/accent normalization

---

## 🔑 Configurable Keys (`config/config.yaml`)
- `folder`: main raw data folder housing 6 NIHExPORTER subfolders (e.g. ClinicalStudies, Patents, PRJ, PRJABS, PUB, PUBLINK)
- `subfolders`: houses NIHExPORTER raw (.csv) datasets from a single source (e.g. PRJ)
- `rename_columns_map`: renames an old column name with a new given column name (e.g. CORE_PROJECT_NUM: PROJECT_NUMBER renames CORE_PROJECT_NUM with PROJECT_NUMBER)
- `treatment`: keyword, e.g. CRISPR, gene therapy, ASOs
- `disease`: keyword, e.g. rare diseases, Batten Disease
- `text_columns`: target columns with text to search for keywords 
- `ml_columns`: selected columns to include in the exported ML training-ready dataset
- `output_dir`: output directory
- `dedup_summary_csv_path`: output path and filename for the deduplication summary table 
- `parallel`: toggle for parallel processing 
- `workers`: number of processing cores that can run concurrently, used to define the number of data chunks during parallel processing 
- `loglevel`: log level
- `remove_duplicates`: toggle to remove duplicates

---

## 📘 Data Dictionary (`MLdf_training.csv`)

| Column              | Description                                       |
|---------------------|---------------------------------------------------|
| APPLICATION_ID      | Unique funding application ID                    |
| PHR                 | Public Health Relevance Statement                |
| PROJECT_TERMS       | NIH-defined project descriptors                  |
| PROJECT_TITLE       | Project title                                    |
| ABSTRACT_TEXT       | Project abstract                                 |
| total count         | Total matched keywords (treatment + disease)     |
| total unique count  | Unique keyword matches                           |
| flagged             | List of matched keyword terms per row            |

---
