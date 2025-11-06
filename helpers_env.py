import os, boto3
import re, uuid

def sanitize_name(name: str) -> str:
    s = name.lower().strip().replace(" ", "-")
    s = re.sub(r"[^a-z0-9-]", "", s)
    s = re.sub(r"-+", "-", s).strip("-")
    if not s or not s[0].isalpha():
        s = "u-" + s
    return s

def build_api_name(display_name: str, suffix: str, prefix="ghc25", max_len=40):
    base = sanitize_name(display_name)
    candidate = f"{prefix}-{base}-{suffix}"
    if len(candidate) > max_len:
        overflow = len(candidate) - max_len
        base = base[:-overflow] if overflow < len(base) else base[: max(1, len(base)-overflow)]
        candidate = f"{prefix}-{base}-{suffix}"
    return candidate, base

def get_region():
    return os.environ.get("AWS_REGION") or os.environ.get("AWS_DEFAULT_REGION")

def whoami(region: str):
    sts = boto3.client("sts", region_name=region)
    ident = sts.get_caller_identity()
    acct, arn = ident["Account"], ident["Arn"]
    return acct, arn

def validate_identifiers(first_name_prefix: str, birth_mmdd: str):
    if not first_name_prefix or first_name_prefix == "NAME":
        raise ValueError("ERROR: Please replace 'NAME' with the first 4 letters of your first name")
    if not birth_mmdd or birth_mmdd == "MMDD":
        raise ValueError("ERROR: Please replace 'MMDD' with your birth month and day (e.g., '0315' for March 15)")