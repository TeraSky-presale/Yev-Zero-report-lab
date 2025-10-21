# zero_report_parse.py — Modularized ETL v1.1 (Titan-only, cleaned)

import argparse, io, json, re, sys, unicodedata, hashlib, datetime
import boto3
import pdfplumber
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from io import BytesIO

# ----------------------------
# PDF LOADER
# ----------------------------

HEBREW_RE = re.compile(r"[\u0590-\u05FF]")

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
# TEXT & METADATA ANALYSIS
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
# STRUCTURED FIELD EXTRACTION
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
            for raw_line in raw_text.split("\n"):
                line, flipped = clean_text(raw_line), flip_line(clean_text(raw_line))
                for key, labels in field_patterns.items():
                    if key in seen_keys:
                        continue
                    for label in labels:
                        fwd_pat = rf"{label}\s*[:\-]?\s*(.+)"
                        rev_pat = rf"(.+)\s*[:\-]?\s*{label}"
                        for pat in (fwd_pat, rev_pat):
                            match = re.search(pat, line) or re.search(pat, flipped)
                            if match:
                                val = match.group(1).strip().strip(".")
                                if re.search(pat, flipped):
                                    val = val[::-1]  # unflip value only
                                extracted[key] = val
                                seen_keys.add(key)
                                print(f"[DEBUG] ✓ Matched {key}: {val}")
                                break
    return extracted

# ----------------------------
# S3 OUTPUT HANDLERS
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
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)
    boto3.client("s3").put_object(Bucket=bucket, Key=key, Body=buf.read())
    print(f"[INFO] ✅ Wrote Parquet: s3://{bucket}/{key}")

# ----------------------------
# LLM CONTEXT (PARAGRAPH) + TITAN CALL
# ----------------------------

def extract_target_paragraph(pdf_bytes: bytes, page_num: int = 2) -> str:
    """Grab the block immediately after 'לתשומת לב הגוף המממן', including bullet lines."""
    def normalize(line: str) -> str:
        line = unicodedata.normalize("NFKC", line)
        line = re.sub(r"[\u200f\u200e]", "", line)
        return line.strip()

    anchors = [
        "לתשומת לב הגוף המממן",
        "לתשומת לב הגוף המממן:",
        ":ןמממה ףוגה בל תמושתל",
    ]

    with pdfplumber.open(io.BytesIO(pdf_bytes)) as pdf:
        page_text = pdf.pages[page_num].extract_text() or ""
        lines = page_text.split("\n")

        for i, raw in enumerate(lines):
            norm = normalize(raw)
            if any(a in norm for a in anchors):
                block = []
                # collect up to the next blank line OR up to 12 lines (sane cap)
                for j in range(i + 1, min(i + 1 + 12, len(lines))):
                    nxt = normalize(lines[j])
                    if not nxt:
                        break
                    # Keep bullets but strip the symbol
                    nxt = nxt.lstrip("•").strip()
                    block.append(nxt)
                paragraph = " ".join(block)
                print(f"[DEBUG] Extracted paragraph: {paragraph}")
                return paragraph

    print("[WARN] No context paragraph found under the anchor.")
    return ""


def ask_bedrock_titan(prompt: str, region: str = "eu-central-1", model_id: str = "amazon.titan-text-express-v1") -> str:
    """Calls Amazon Titan Text Express with a plain prompt and returns outputText."""
    br = boto3.client("bedrock-runtime", region_name=region)
    body = {
        "inputText": prompt,
        "textGenerationConfig": {
            "maxTokenCount": 400,
            "temperature": 0.2,
            "topP": 0.9,
            "stopSequences": []
        }
    }
    resp = br.invoke_model(
        modelId=model_id,
        body=json.dumps(body),
        contentType="application/json",
        accept="application/json"
    )
    payload = json.loads(resp["body"].read().decode("utf-8"))
    return (payload.get("results") or [{}])[0].get("outputText", "").strip()

def _hebrew_word_to_int(word: str) -> int:
    m = {
        "אחת": 1, "אחד": 1,
        "שתיים": 2, "שתי": 2, "שניים": 2, "שני": 2,
        "שלוש": 3, "שלושה": 3,
        "ארבע": 4, "ארבעה": 4,
        "חמש": 5, "חמישה": 5,
        "שש": 6, "שישה": 6,
        "שבע": 7, "שבעה": 7,
        "שמונה": 8, "שמונהְ": 8,
        "תשע": 9, "תשעה": 9,
        "עשר": 10, "עשרה": 10,
        "אחת עשרה": 11, "אחד עשר": 11,
        "שתים עשרה": 12, "שנים עשר": 12,
    }
    return m.get(word.strip(), 0)

def _extract_numbers_fallback(text: str) -> dict:
    """
    Pull floors/units from Hebrew text via regex heuristics.
    Looks for digits or words near קומ(ה|ות) and יח\"ד/יחידות דיור.
    """
    floors = 0
    units = 0

    # Floors: numeric
    m = re.search(r"(?:תוספת|הוספת)?\s*(\d+)\s*(?:קומות?|קומה)", text)
    if m:
        floors = int(m.group(1))

    # Floors: Hebrew word (e.g., 'קומה אחת', 'שלוש קומות')
    if floors == 0:
        m = re.search(r"(?:תוספת|הוספת)?\s*([א-ת ]+?)\s*(?:קומה|קומות)", text)
        if m:
            floors = _hebrew_word_to_int(m.group(1))

    # Units: numeric
    m = re.search(r"(\d+)\s*(?:יח\"?ד|יחידות\s*דיור)", text)
    if m:
        units = int(m.group(1))

    return {"new_floors_count": floors, "new_residential_units": units}


def get_llm_additions_titan(context: str, region: str = "eu-central-1") -> dict:
    """Extracts structured 'project additions' from context using Titan and returns a compact JSON-ready dict."""
    if not context or not context.strip():
        print("[WARN] ❌ Context paragraph not found or empty.")
        return {}

    prompt = (
        "אתה מקבל פסקה ממסמך פרויקט בנייה. החזר אך ורק JSON תקין (ללא טקסט מסביב), עם המפתחות הבאים:\n"
        "new_floors_count (int), new_residential_units (int), additions_list_he (array of strings), summary_he (string קצר בעברית).\n"
        "נתמקד רק במה יתווסף לבניין (קומות/יח\"ד/מרפסות/ממ\"דים/שיפוץ/חיזוק). אם לא ידוע מספרים, החזר 0.\n\n"
        "פסקה:\n"
        f"\"\"\"{context}\"\"\""
    )

    try:
        raw = ask_bedrock_titan(prompt, region=region)  # uses Titan Text Express
        # Try strict JSON parse first
        try:
            obj = json.loads(raw)
        except Exception:
            # Fallback: extract a JSON block if the model wrapped it in prose
            m = re.search(r"\{.*\}", raw, flags=re.DOTALL)
            obj = json.loads(m.group(0)) if m else {}

        if not isinstance(obj, dict):
            obj = {}

        # Normalize + defaults
        result = {
            "new_floors_count": int(obj.get("new_floors_count", 0) or 0),
            "new_residential_units": int(obj.get("new_residential_units", 0) or 0),
            "additions_list_he": obj.get("additions_list_he") or [],
            "summary_he": (obj.get("summary_he") or "").strip()
        }

        # Compose final fields you store
        out = {
            "llm_project_additions_model": "amazon.titan-text-express-v1",
            "llm_project_additions_he": result["summary_he"],
            "llm_additions": result
        }

        # Fallback: if the model didn't provide numbers, try to lift them from text deterministically.
        if (out["llm_additions"].get("new_floors_count", 0) == 0 or
            out["llm_additions"].get("new_residential_units", 0) == 0):
            fallback = _extract_numbers_fallback(context)
            if out["llm_additions"].get("new_floors_count", 0) == 0 and fallback["new_floors_count"]:
                out["llm_additions"]["new_floors_count"] = fallback["new_floors_count"]
            if out["llm_additions"].get("new_residential_units", 0) == 0 and fallback["new_residential_units"]:
                out["llm_additions"]["new_residential_units"] = fallback["new_residential_units"]

        print(f"[INFO] ✅ Titan structured: {json.dumps(out['llm_additions'], ensure_ascii=False)}")
        return out

    except Exception as e:
        print(f"[WARN] ❌ LLM extraction failed (Titan): {e}")
        return {}

# ----------------------------
# MAIN ETL FLOW
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
    ap.add_argument("--ENABLE_BEDROCK_TEXT", default="false", help="true to enable LLM paragraph analysis")

    args, unknown = ap.parse_known_args()
    enable_bedrock = args.ENABLE_BEDROCK_TEXT.lower() == "true"

    print("[DEBUG] ▶ ETL Script v1.1 Started")
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

    # Metadata
    doc_id = generate_doc_id(args.SOURCE_BUCKET, args.SOURCE_KEY, head_meta["etag"])
    ingest_date = datetime.date.today().isoformat()
    metadata = extract_metadata(pdf_bytes, args.SOURCE_BUCKET, args.SOURCE_KEY, head_meta)
    metadata.update({"doc_id": doc_id, "ingest_date": ingest_date})

    # Save metadata
    save_json_to_s3(metadata, args.OUT_BUCKET, f"{args.STAGING_JSON_PREFIX}/ingest_date_partition={ingest_date}/doc_id_partition={doc_id}/metadata.json")
    save_parquet_to_s3(pd.DataFrame([metadata]), args.OUT_BUCKET, f"{args.STAGING_PARQUET_PREFIX}/ingest_date_partition={ingest_date}/doc_id_partition={doc_id}/metadata.parquet")

    # Structured data
    structured = extract_structured_data(pdf_bytes)

    # Optional LLM enrichment
    if enable_bedrock:
        context_paragraph = extract_target_paragraph(pdf_bytes, page_num=2)  # page 3
        structured.update(get_llm_additions_titan(context_paragraph))

    # Save structured data
    save_json_to_s3(structured, args.OUT_BUCKET, f"{args.PROCESSED_JSON_PREFIX}/ingest_date_partition={ingest_date}/doc_id_partition={doc_id}/data.json")
    save_parquet_to_s3(
        pd.DataFrame([{**structured, "doc_id": doc_id, "ingest_date": ingest_date}]),
        args.OUT_BUCKET,
        f"{args.PROCESSED_PARQUET_PREFIX}/ingest_date_partition={ingest_date}/doc_id_partition={doc_id}/data.parquet"
    )

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
