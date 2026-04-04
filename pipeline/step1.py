import hashlib

from pipeline.quality import score_data_quality, summarize_quality
from pipeline.utils import normalize


def detect_pii_action(col):
    norm = normalize(col)

    keyword_rules = {
        "name": "drop",
        "email": "drop",
        "phone": "drop",
        "address": "drop",
        "id": "hash",
    }

    for key, action in keyword_rules.items():
        if key == "id":
            if norm.endswith("id"):
                return action
        elif key in norm:
            return action

    return None


def hash_value(val):
    return hashlib.sha256(str(val).encode()).hexdigest()


def handle_pii(df):
    df = df.copy()
    report = []

    for col in df.columns:
        action = detect_pii_action(col)

        if action == "drop":
            df.drop(columns=[col], inplace=True)
            report.append({"column": col, "action": "dropped"})

        elif action == "hash":
            df[col] = df[col].apply(hash_value)
            report.append({"column": col, "action": "hashed"})

    return df, report


def run_step1(df):
    reports = {}

    df, pii_report = handle_pii(df)
    reports["pii"] = pii_report

    df = score_data_quality(df)
    reports["quality"] = summarize_quality(df)

    return df, reports
