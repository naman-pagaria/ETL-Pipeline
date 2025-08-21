# 🛠️ ETL-Pipeline

A Python-based **ETL (Extract, Transform, Load) pipeline** for automating Excel data processing.  
The pipeline extracts data from Excel workbooks, tidies it into a structured format, optionally loads it into MySQL, and generates visual reports (PDF graphs).

---

## 🚀 Features
- Extracts values from Excel sheets containing **“Reg”** / **“Em”** models  
- Automatically detects rows around the **“Min” marker**  
- Transforms messy Excel sheets into a clean, tidy dataset  
- Saves results to:
  - Excel (`Model_YYYYMMDD.xlsx`)
  - MySQL table (`analystdata`, optional)
  - Merged graphs PDF (`Graphs.pdf`)  
- OS-agnostic (works on Mac, Linux, Windows)  
- Configurable via **CLI arguments** or **.env file**  

---

## 📂 Project Structure
ETL-Pipeline/
│── etl_pipeline.py      # Main ETL script
│── requirements.txt     # Dependencies
│── README.md            # Project documentation
│── .gitignore           # Ignore cache, env, outputs
│── .env.example         # Example environment variables
│── input/               # Place your raw Excel files here
│── output/
│    ├── data/           # Generated tidy Excel file
│    ├── plots/          # Per-row chart PDFs
│    └── final/          # Combined Graphs.pdf

---

## ⚡ Quick Start

### 1. Clone the repo
git clone https://github.com/naman-pagaria/ETL-Pipeline.git
cd ETL-Pipeline

### 2. Install dependencies
pip install -r requirements.txt

### 3. Prepare input/output folders
mkdir -p input output/data output/plots output/final

### 4. Run the pipeline
python etl_pipeline.py --skip-mysql

- Put your `.xlsx` files inside **`input/`**
- Results will be written to **`output/`**

---

## ⚙️ Options
You can customize via CLI:

python etl_pipeline.py   --input ./input   --out-data ./output/data   --out-plots ./output/plots   --out-final ./output/final   --sheet-keys Reg Em   --pattern "*.xlsx"   --skip-mysql

Or via `.env` file (see `.env.example`).

---

## 🗄️ MySQL (Optional)
If you want to load data into MySQL, set up a `.env` file:

MYSQL_HOST=127.0.0.1
MYSQL_DB=data
MYSQL_USER=root
MYSQL_PASSWORD=your_password

Run without `--skip-mysql` to insert into MySQL.

---

## 📊 Outputs
- `output/data/Model_YYYYMMDD.xlsx` → tidy dataset  
- `output/final/Graphs.pdf` → merged bar chart PDF  
- `output/plots/` → per-ticker/type charts  

---

## 📝 License
MIT License © 2025 [Naman Pagaria](https://github.com/naman-pagaria)
