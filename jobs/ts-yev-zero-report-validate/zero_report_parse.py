# zero_report_parse.py
# v0.4 — Fixes for S3 argument usage + Athena compatibility

import argparse, io, json, re, sys, unicodedata, hashlib, datetime
import boto3
import pdfplumber
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO

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
    return obj["Body"].read(), {"etag": head["ETag"].strip('"'), "size": head["ContentLength"]}

def extract_text_sample(pdf_bytes: bytes, max_pages: int = 10):
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        page_count = len(pdf.pages)
        take = min(page_count, max_pages)
        return "\n".join([pdf.pages[i].extract_text() or "" for i in range(take)]), page_count

def has_hebrew(text: str) -> bool:
    return bool(HEBREW_RE.search(text))

def extract_metadata(pdf_bytes: bytes, source_bucket: str, source_key: str, head_meta: dict) -> dict:
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        md = pdf.metadata or {}
        norm = {}
        for k, v in md.items():
            if v is None:
                continue
            key = str(k).strip().lower().replace(" ", "_").replace("-", "_")
            norm[key] = str(v)
        norm.update({
            "page_count": len(pdf.pages),
            "source_bucket": source_bucket,
            "source_key": source_key,
            "size_bytes": head_meta.get("size"),
            "etag": head_meta.get("etag"),
        })
        return norm

def save_metadata_as_parquet(metadata: dict, doc_id: str, ingest_date: str, bucket: str, base_prefix: str):
    s3 = boto3.client("s3")
    output_df = pd.DataFrame([{
        "doc_id": doc_id,
        "bucket": metadata.get("source_bucket"),
        "key": metadata.get("source_key"),
        "size_bytes": metadata.get("size_bytes"),
        "pages": metadata.get("page_count"),
        "hebrew_detected": True,  # flatten this field
        "metadata": metadata,
        "ingest_date": ingest_date  # ← this is the column
    }])

    # Convert to parquet in memory
    table = pa.Table.from_pandas(output_df)
    buffer = BytesIO()
    pq.write_table(table, buffer, compression='snappy')
    buffer.seek(0)

    # Save to S3 using partitioned path
    parquet_key = f"{base_prefix}/ingest_date_partition={ingest_date}/doc_id_partition={doc_id}/metadata.parquet"
    s3.put_object(Bucket=bucket, Key=parquet_key, Body=buffer.read())
    print(f"✅ Stored Parquet: s3://{bucket}/{parquet_key}")


# --- Main Flow ---

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--SOURCE_BUCKET", required=True)
    ap.add_argument("--SOURCE_KEY", required=True)
    ap.add_argument("--OUT_BUCKET", required=True)
    ap.add_argument("--STAGING_JSON_PREFIX", required=True)
    ap.add_argument("--STAGING_PARQUET_PREFIX", required=True)
    args, unknown = ap.parse_known_args()

    print(f"[DEBUG] Starting Option C v0.4")
    if unknown:
        print(f"[DEBUG] Ignored Glue args: {unknown}", flush=True)

    try:
        pdf_bytes, meta = load_pdf_bytes(args.SOURCE_BUCKET, args.SOURCE_KEY)
    except Exception as e:
        print(f"[ERROR] Failed to load PDF: {e}", file=sys.stderr)
        sys.exit(2)

    try:
        sample_text, page_count = extract_text_sample(pdf_bytes)
    except Exception as e:
        print(f"[ERROR] Failed to parse PDF: {e}", file=sys.stderr)
        sys.exit(3)

    if not sample_text.strip():
        print("[ERROR] No text found in PDF (possible scanned file)", file=sys.stderr)
        sys.exit(4)

    if not has_hebrew(sample_text):
        print("[ERROR] No Hebrew detected in text", file=sys.stderr)
        sys.exit(5)

    doc_id = hashlib.sha256(f"{args.SOURCE_BUCKET}/{args.SOURCE_KEY}/{meta['etag']}".encode()).hexdigest()[:16]
    ingest_date = datetime.date.today().isoformat()

    doc_meta = extract_metadata(pdf_bytes, args.SOURCE_BUCKET, args.SOURCE_KEY, meta)
    doc_meta["doc_id"] = doc_id
    doc_meta["ingest_date"] = ingest_date

    json_key = f"{args.STAGING_JSON_PREFIX}/ingest_date={ingest_date}/doc_id={doc_id}/metadata.json"
    boto3.client("s3").put_object(
        Bucket=args.OUT_BUCKET,
        Key=json_key,
        Body=json.dumps(doc_meta, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json"
    )
    print(f"✅ Stored metadata JSON: s3://{args.OUT_BUCKET}/{json_key}")

    save_metadata_as_parquet(doc_meta, doc_id, ingest_date, args.OUT_BUCKET, args.STAGING_PARQUET_PREFIX)

    print(json.dumps({
        "status": "OK",
        "doc_id": doc_id,
        "ingest_date": ingest_date,
        "bucket": args.SOURCE_BUCKET,
        "key": args.SOURCE_KEY,
        "size_bytes": meta["size"],
        "pages": page_count
    }, ensure_ascii=False))

if __name__ == "__main__":
    main()
