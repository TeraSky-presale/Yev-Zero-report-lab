## 📄 Zero Report Lab – ETL Metadata Extractor

This AWS Glue Python Shell job extracts and validates metadata from Hebrew PDF files representing "Zero Reports" (דו"ח אפס) uploaded to an S3 bucket. The job is designed for extensibility, following a **pilot-light → full-feature** architecture.

---

### 🚀 Project Purpose

- ✅ Extract metadata (e.g., title, author, creation date) from PDFs stored in S3  
- ✅ Validate Hebrew language presence for contextual relevance  
- ✅ Store structured JSON metadata in a foldered S3 hierarchy  
- 🔜 Future: extract text per page, image content, semantic context (intent), and more  

---

### 🗂️ Folder Structure

```bash
s3://ts-yev-zero-report/
├── raw/                         # Source PDF uploads
│   └── <uuid>.pdf
├── scripts/                     # Glue job Python scripts
│   └── zero_report_parse.py
└── staging/                     # ETL output
    └── ingest_date=YYYY-MM-DD/
        └── doc_id=<uuid>/
            └── metadata.json
🧠 Glue Job Configuration
Parameter	Value
Name	ts-yev-zero-report-validate
Job Type	Python Shell
Script Location	s3://ts-yev-zero-report/scripts/zero_report_parse.py
Glue Version	4.0
Python Version	3.9
Max Capacity	0.0625 (1/16 DPU)
IAM Role	ts-yev-glue-role-zero-report

Default Arguments
bash
Copy code
--SOURCE_BUCKET=ts-yev-zero-report
--SOURCE_KEY=raw/0a927e9e-02ab-4759-8dff-cc0653ff0cad.pdf
--STAGING_PREFIX=staging
--additional-python-modules=pdfplumber,pdfminer.six,pillow
🧪 How to Run
Manual Trigger (AWS Console):

Upload your PDF to s3://ts-yev-zero-report/raw/

Set the correct --SOURCE_KEY in Glue job parameters.

Press Run Job in the AWS Glue console.

Check output in the staging/ folder and logs in CloudWatch.

📦 Python Script (zero_report_parse.py)
Performs S3 download of the PDF

Validates Hebrew text via unicode analysis

Extracts document-level metadata

Stores results in structured metadata.json

✅ Example Output:

json
Copy code
{
  "status": "OK",
  "doc_id": "74a812f56fb8236f",
  "bucket": "ts-yev-zero-report",
  "key": "raw/0a927e9e-02ab-4759-8dff-cc0653ff0cad.pdf",
  "size_bytes": 4225829,
  "pages": 35,
  "validations": {
    "hebrew_detected": true
  },
  "metadata": {
    "Title": "Some Title",
    "Author": "Someone",
    "CreationDate": "D:20230101120000+02'00'"
  }
}
🛣️ Roadmap
 Pilot-light: validate PDF and store metadata

 Text extraction per page (with layout structure)

 Embedded image extraction to S3 (with image-to-text hooks)

 NLP-driven context analysis ("intent")

 Convert to cleaned Parquet with structured schema

 Enable automated Lambda-trigger on PDF upload

 GitHub → Glue version control integration

👨‍💻 Local Development Setup
bash
Copy code
git clone https://github.com/TeraSky-presale/Yev-Zero-report-lab.git
cd Yev-Zero-report-lab
pip install -r requirements.txt
python jobs/ts-yev-zero-report-validate/zero_report_parse.py \
    --SOURCE_BUCKET ts-yev-zero-report \
    --SOURCE_KEY raw/example.pdf \
    --STAGING_PREFIX staging
🧾 Notes & Troubleshooting
Glue Python Shell jobs require script path to be S3, not Git (until version control is working).

Hebrew detection is based on Unicode ranges (\u0590–\u05FF).

Metadata is extracted using pdfplumber.metadata.

Logs are available in CloudWatch per job run.

🔐 Permissions Required
s3:GetObject, s3:PutObject

logs:CreateLogGroup, logs:PutLogEvents, etc.