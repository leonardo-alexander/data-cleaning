# Data Cleaning Pipeline API

A simple, rule-based data cleaning pipeline designed to transform raw data into standardized datasets.

The goal is to take raw, messy CSV data and turn it into something structured, consistent, usable, and transparant.

---

## What this does

Given a CSV file, the API can:

- standardize column names  
- remove or hash sensitive data (PII)  
- detect and remove duplicates  
- validate inputs (numeric, ranges, etc.)  
- split invalid and missing rows  
- normalize values (gender, boolean, text)  
- convert types (numbers, dates)  
- assign a data quality score  
- return a cleaned dataset + report  

---

## Pipeline Overview

### Step 1 (Basic)
- schema standardization  
- PII handling (drop / hash)  
- initial quality scoring  

### Step 2 (Advanced)
- duplicate removal  
- validation  
- missing value handling  
- type conversion  
- value normalization  
- final dataset + stats  

---

## API Endpoints

### 1. Process (preview + report)

**Endpoint**

    POST /process

**Query Parameters**

    mode = step1 | full

**Returns**
- cleaning report  
- preview (first 5 rows)  
- invalid/missing stats (step2/full)  

---

### 2. Download cleaned dataset

**Endpoint**

    POST /process/download

**Downloads**
- cleaned_step1.csv 
- cleaned_full.csv

---

## Example Flow

1. Upload CSV  
2. Get preview + report  
3. Download cleaned dataset  

---

## Tech Stack

- Python  
- FastAPI  
- Pandas  

No ML/LLM — everything is rule-based for simplicity and explainability.

---

## Project Structure

    /
    ├── api.py
    ├── pipeline/
    │   ├── step1.py
    │   ├── step2.py
    │   ├── quality.py
    │   ├── utils.py
    ├── data/              # ignored (outputs)
    ├── requirements.txt

---

## How to run

### 1. Install dependencies

    pip install -r requirements.txt

### 2. Run API

    uvicorn api:app --reload

### 3. Open docs

    http://127.0.0.1:8000/docs

---

## Notes

- Only CSV is supported for now  
- Max dataset size is limited (to avoid abuse)  
- Output datasets are not stored (streamed directly)  

---

## Why I built this

Working with raw data is often repetitive and inconsistent. This project was built to standardize common cleaning steps into a simple, reusable pipeline.

The focus is:
- simple rules  
- transparency  
- predictable behavior  
