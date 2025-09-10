# zero_report_parse.py
# v0.3 — validate + metadata only, Option C (Hybrid) foldering
# Outputs:
#   staging/ingest_date=YYYY-MM-DD/doc_id=<doc_id>/metadata.json

import argparse, io, json, re, sys, unicodedata, hashlib, datetime
import boto3
import pdfplumber

# --- helpers -----------------------------------------------------------------

HEBREW_RE = re.compile(r"[\u0590-\u05FF]")

def strip_diacritics(text: str) -> str:
    return "".join(ch for ch in unicodedata.normalize("NFD", text) if unicodedata.category(ch) != "Mn")

def load_pdf_bytes(bucket: str, key: str):
    """Return (pdf_bytes, head_meta). head_meta has 'etag', 'size'."""
    s3 = boto3.client("s3")
    head = s3.head_object(Bucket=bucket, Key=key)
    if not key.lower().endswith(".pdf"):
        raise ValueError(f"Object key does not end with .pdf: {key}")
    if head.get("ContentLength", 0) < 1024:
        raise ValueError(f"Object appears too small to be a valid PDF ({head.get('ContentLength')} bytes).")
    obj = s3.get_object(Bucket=bucket, Key=key)
    return obj["Body"].read(), {"etag": head["ETag"].strip('"'), "size": head["ContentLength"]}

def extract_text_sample(pdf_bytes: bytes, max_pages: int = 10):
    """Return (concatenated_text_first_N_pages, page_count)."""
    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        page_count = len(pdf.pages)
        take = min(page_count, max_pages)
        chunks = []
        for i in range(take):
            txt = pdf.pages[i].extract_text() or ""
            chunks.append(txt)
        return "\n".join(chunks), page_count

def has_hebrew(text: str) -> bool:
    return bool(HEBREW_RE.search(text))

def extract_metadata(pdf_bytes: bytes, source_bucket: str, source_key: str, head_meta: dict) -> dict:
    """Extract PDF metadata (as many fields as available) + add computed fields."""
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

# --- main --------------------------------------------------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--SOURCE_BUCKET", required=True)
    ap.add_argument("--SOURCE_KEY", required=True)
    ap.add_argument("--OUT_BUCKET", required=True)
    # kept for consistency; not used to build the Option C path directly
    ap.add_argument("--STAGING_PREFIX", default="staging")

    # Accept our args; ignore Glue-injected extras
    args, unknown = ap.parse_known_args()
    print(f"[DEBUG] start v0.3 validate+metadata (Option C)", flush=True)
    print(f"[DEBUG] args: {args}", flush=True)
    if unknown:
        print(f"[DEBUG] ignoring extra args from Glue: {unknown}", flush=True)

    s3 = boto3.client("s3")

    # 1) Load + validations
    try:
        pdf_bytes, meta = load_pdf_bytes(args.SOURCE_BUCKET, args.SOURCE_KEY)
    except Exception as e:
        print(f"[ERROR] Failed to load S3 object: {e}", file=sys.stderr, flush=True)
        sys.exit(2)

    try:
        sample_text, page_count = extract_text_sample(pdf_bytes, max_pages=10)
    except Exception as e:
        print(f"[ERROR] Failed to open/parse PDF: {e}", file=sys.stderr, flush=True)
        sys.exit(3)

    if not sample_text.strip():
        print("[ERROR] No extractable text found in first pages (possible scanned PDF without OCR).", file=sys.stderr, flush=True)
        sys.exit(4)
    if not has_hebrew(sample_text):
        print("[ERROR] Sample text does not contain Hebrew characters.", file=sys.stderr, flush=True)
        sys.exit(5)

    # 2) doc_id + ingest_date (UTC date for partition)
    doc_id = hashlib.sha256(f"{args.SOURCE_BUCKET}/{args.SOURCE_KEY}/{meta['etag']}".encode()).hexdigest()[:16]
    ingest_date = datetime.date.today().isoformat()

    # 3) Extract & store metadata JSON (Option C pathing)
    doc_meta = extract_metadata(pdf_bytes, args.SOURCE_BUCKET, args.SOURCE_KEY, meta)
    doc_meta["doc_id"] = doc_id
    doc_meta["ingest_date"] = ingest_date

    # staging/ingest_date=YYYY-MM-DD/doc_id=<doc_id>/metadata.json
    meta_key = f"{args.STAGING_PREFIX}/ingest_date={ingest_date}/doc_id={doc_id}/metadata.json"
    s3.put_object(
        Bucket=args.OUT_BUCKET,
        Key=meta_key,
        Body=json.dumps(doc_meta, ensure_ascii=False, indent=2).encode("utf-8"),
        ContentType="application/json"
    )
    print(f"[INFO] wrote metadata JSON: s3://{args.OUT_BUCKET}/{meta_key}", flush=True)

    # 4) Final summary
    summary = {
        "status": "OK",
        "doc_id": doc_id,
        "ingest_date": ingest_date,
        "bucket": args.SOURCE_BUCKET,
        "key": args.SOURCE_KEY,
        "size_bytes": meta["size"],
        "pages": page_count,
        "outputs": {"metadata_json": f"s3://{args.OUT_BUCKET}/{meta_key}"}
    }
    print(json.dumps(summary, ensure_ascii=False), flush=True)

if __name__ == "__main__":
    main()
