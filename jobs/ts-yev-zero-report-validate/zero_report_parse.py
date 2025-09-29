# zero_report_parse.py — Modularized ETL v1.0

import argparse, io, json, re, sys, unicodedata, hashlib, datetime
import boto3
import pdfplumber
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO

# ----------------------------
# 📌 PDF LOADER
# ----------------------------

HEBREW_RE = re.compile(r"[\u0590-\u05FF]")

def strip_diacritics(text: str) -> str:
    return "".join(ch for ch in unicodedata.normalize("NFD", text) if unicodedata.category(ch) != "Mn")

def load_pdf_bytes(bucket: str, key: str):
    s3 = boto3.client("s3")
    head = s3.head_object(Bucket=bucket, Key=key)

    if not key.lower().endswith(".pdf"):
        raise ValueError(f"Object key does not end with .pdf: {key}")
    if head.get("ContentLength", 0) < 1024:
        raise ValueError(f"Object too small to be a valid PDF ({head.get('ContentLength')} bytes).")

    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read(), {
        "etag": head["ETag"].strip('"'),
        "size": head["ContentLength"]
    }

def generate_doc_id(bucket: str, key: str, etag: str) -> str:
    raw = f"{bucket}/{key}/{etag}"
    return hashlib.sha256(raw.encode()).hexdigest()[:16]

# ----------------------------
# 📌 TEXT & METADATA ANALYSIS
# ----------------------------

def extract_text_sample(pdf_bytes: bytes, max_pages: int = 10):
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        page_count = len(pdf.pages)
        pages = [pdf.pages[i].extract_text() or "" for i in range(min(max_pages, page_count))]
        return "\n".join(pages), page_count

def has_hebrew(text: str) -> bool:
    return bool(HEBREW_RE.search(text))

def extract_metadata(pdf_bytes: bytes, bucket: str, key: str, head_meta: dict) -> dict:
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        md = pdf.metadata or {}
        normalized = {
            str(k).strip().lower().replace(" ", "_").replace("-", "_"): str(v)
            for k, v in md.items() if v is not None
        }
        normalized.update({
            "page_count": len(pdf.pages),
            "source_bucket": bucket,
            "source_key": key,
            "size_bytes": head_meta.get("size"),
            "etag": head_meta.get("etag"),
        })
        return normalized

# ----------------------------
# 📌 STRUCTURED FIELD EXTRACTION
# ----------------------------

def extract_structured_data(pdf_bytes: bytes, max_pages: int = 4) -> dict:
    field_patterns = {
        "block": ["גוש"],
        "plot": ["חלקה"],
        "registered_area": ["שטח רשום", "רשום שטח"],
        "address": ["כתובת"]
    }

    def clean_text(text):
        text = unicodedata.normalize("NFKC", text)
        return re.sub(r"[\u200f\u200e]", "", text).strip()

    def flip_line(line):
        return line[::-1]

    extracted, seen_keys = {}, set()
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        for page_num in range(min(len(pdf.pages), max_pages)):
            print(f"\n[DEBUG] Scanning Page {page_num + 1}")
            raw_text = pdf.pages[page_num].extract_text() or ""
            for i, raw_line in enumerate(raw_text.split("\n")):
                print(f"[DEBUG] Raw Line {i}: {repr(raw_line)}")
                line, flipped = clean_text(raw_line), flip_line(clean_text(raw_line))
                print(f"[DEBUG] Flipped Line {i}: {repr(flipped)}")

                for key, labels in field_patterns.items():
                    if key in seen_keys:
                        continue
                    for label in labels:
                        # Try both directions
                        fwd_pat = rf"{label}\s*[:\-]?\s*(.+)"
                        rev_pat = rf"(.+)\s*[:\-]?\s*{label}"
                        for pat in [fwd_pat, rev_pat]:
                            match = re.search(pat, line) or re.search(pat, flipped)
                            if match:
                                val = match.group(1).strip().strip(".")
                                if re.search(pat, flipped):
                                    val = val[::-1]  # Unflip value only
                                extracted[key] = val
                                seen_keys.add(key)
                                print(f"[DEBUG] ✓ Matched {key}: {val}")
                                break
    return extracted

# ----------------------------
# 📌 S3 OUTPUT HANDLERS
# ----------------------------

def save_json_to_s3(obj: dict, bucket: str, key: str):
    boto3.client("s3").put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json"
    )
    print(f"[INFO] ✅ Wrote JSON: s3://{bucket}/{key}")

def save_parquet_to_s3(df: pd.DataFrame, bucket: str, key: str):
    table = pa.Table.from_pandas(df)
    buf = BytesIO()
    pq.write_table(table, buf, compression='snappy')
    buf.seek(0)
    boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=buf.read())
    print(f"[INFO] ✅ Wrote Parquet: s3://{bucket}/{key}")

# ----------------------------
# 📌 MAIN ETL FLOW
# ----------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--SOURCE_BUCKET", required=True)
    ap.add_argument("--SOURCE_KEY", required=True)
    ap.add_argument("--OUT_BUCKET", required=True)
    ap.add_argument("--STAGING_JSON_PREFIX", required=True)
    ap.add_argument("--STAGING_PARQUET_PREFIX", required=True)
    ap.add_argument("--PROCESSED_JSON_PREFIX", required=True)
    ap.add_argument("--PROCESSED_PARQUET_PREFIX", required=True)
    args, unknown = ap.parse_known_args()

    print(f"[DEBUG] ▶ ETL Script v1.0 Started")
    if unknown:
        print(f"[DEBUG] Ignored Glue args: {unknown}")

    # Load PDF
    try:
        pdf_bytes, head_meta = load_pdf_bytes(args.SOURCE_BUCKET, args.SOURCE_KEY)
    except Exception as e:
        print(f"[ERROR] Failed to load PDF: {e}", file=sys.stderr)
        sys.exit(2)

    # Sample text
    sample_text, page_count = extract_text_sample(pdf_bytes)
    if not sample_text.strip():
        print("[ERROR] Empty PDF content", file=sys.stderr)
        sys.exit(4)
    if not has_hebrew(sample_text):
        print("[ERROR] Hebrew not detected", file=sys.stderr)
        sys.exit(5)

    # Generate ID + Metadata
    doc_id = generate_doc_id(args.SOURCE_BUCKET, args.SOURCE_KEY, head_meta["etag"])
    ingest_date = datetime.date.today().isoformat()
    metadata = extract_metadata(pdf_bytes, args.SOURCE_BUCKET, args.SOURCE_KEY, head_meta)
    metadata.update({"doc_id": doc_id, "ingest_date": ingest_date})

    # Save metadata
    save_json_to_s3(metadata, args.OUT_BUCKET, f"{args.STAGING_JSON_PREFIX}/ingest_date_partition={ingest_date}/doc_id_partition={doc_id}/metadata.json")
    save_parquet_to_s3(pd.DataFrame([metadata]), args.OUT_BUCKET, f"{args.STAGING_PARQUET_PREFIX}/ingest_date_partition={ingest_date}/doc_id_partition={doc_id}/metadata.parquet")

    # Structured field extraction
    structured = extract_structured_data(pdf_bytes)
    save_json_to_s3(structured, args.OUT_BUCKET, f"{args.PROCESSED_JSON_PREFIX}/ingest_date_partition={ingest_date}/doc_id_partition={doc_id}/data.json")
    save_parquet_to_s3(pd.DataFrame([{**structured, "doc_id": doc_id, "ingest_date": ingest_date}]), args.OUT_BUCKET, f"{args.PROCESSED_PARQUET_PREFIX}/ingest_date_partition={ingest_date}/doc_id_partition={doc_id}/data.parquet")

    print(json.dumps({
        "status": "OK",
        "doc_id": doc_id,
        "ingest_date": ingest_date,
        "bucket": args.SOURCE_BUCKET,
        "key": args.SOURCE_KEY,
        "size_bytes": head_meta["size"],
        "pages": page_count
    }, ensure_ascii=False))

if __name__ == "__main__":
    main()
