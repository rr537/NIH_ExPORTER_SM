# 🧬 NIH ExPORTER ML Pipeline

A scalable and modular Snakemake workflow for processing NIH ExPORTER datasets into machine learning–ready training files—optimized for rare disease modeling. Features include robust preprocessing, keyword enrichment via FlashText, and configurable training data generation.

---

## 🚀 Features
-  **Config-driven folder & column mapping**
-  **Keyword tagging** with FlashText: enrichment, stopword control, plural/punctuation/accent handling
-  **Deduplication logic** for clean, reliable outputs
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
- `results/preprocessing_summary.json` → Preprocessing log  

---

## 🧠 Keyword Strategy

FlashText enriches terms with:
- Plural/punctuation/accent normalization
- Omit list + stopword control (from config)

---

## 🔑 Configurable Keys (`config/config.yaml`)
- Folder paths & subfolder definitions
- `drop_col_header_map`, `rename_columns_map`
- `keywords` → `treatment`, `disease`, `omit`
- `text_columns`, `ml_columns`
- Output settings: `output_dir`, `parallel`, `loglevel`, `remove_duplicates`

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
