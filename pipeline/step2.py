import pandas as pd

from pipeline.quality import score_data_quality, summarize_quality
from pipeline.utils import normalize

SYSTEM_COLUMNS = {"QualityScore"}


def remove_exact_duplicates(df):
    before = len(df)
    df_clean = df.drop_duplicates()
    after = len(df_clean)

    return df_clean, {
        "type": "exact",
        "removed_rows": int(before - after),
        "remaining_rows": int(after),
    }


def detect_id_column(df):
    for col in df.columns:
        norm = normalize(col)
        if norm == "id" or norm.endswith("id"):
            return col
    return None


def remove_id_duplicates(df, id_col):
    before = len(df)
    df_clean = df.drop_duplicates(subset=[id_col], keep="first")
    after = len(df_clean)

    return df_clean, {
        "type": "id_based",
        "column": id_col,
        "removed_rows": before - after,
    }


def detect_content_columns(df):
    content_cols = []

    EXCLUDE_COLS = SYSTEM_COLUMNS

    for col in df.columns:
        if col in EXCLUDE_COLS:
            continue

        norm = normalize(col)

        if norm == "id" or norm.endswith("id") or "date" in norm or "time" in norm:
            continue

        if df[col].nunique() > 1:
            content_cols.append(col)

    return content_cols


def remove_content_duplicates(df, subset_cols):
    if len(subset_cols) < 2:
        return df, {"type": "content_based", "columns": subset_cols, "removed_rows": 0}

    before = len(df)
    df_clean = df.copy()
    df_clean = df_clean.fillna("__MISSING__")
    df_clean = df_clean.drop_duplicates(subset=subset_cols)
    df_clean = df_clean.replace("__MISSING__", pd.NA)
    after = len(df_clean)

    return df_clean, {
        "type": "content_based",
        "columns": subset_cols,
        "removed_rows": before - after,
    }


def handle_duplicates(df):
    df = df.copy()
    report = []

    df, r1 = remove_exact_duplicates(df)
    report.append(r1)

    id_col = detect_id_column(df)
    if id_col:
        df, r2 = remove_id_duplicates(df, id_col)
        report.append(r2)

    content_cols = detect_content_columns(df)
    df, r3 = remove_content_duplicates(df, content_cols)
    report.append(r3)

    return df, report


def validate_inputs(df):
    df = df.copy()
    invalid_mask = pd.Series(False, index=df.index)

    for col in df.columns:

        if df[col].dtype == "object":
            try:
                pattern = r"\d+(\.\d+)?"

                is_numeric = df[col].str.fullmatch(pattern, na=False)

                if is_numeric.any():
                    invalid_mask |= ~df[col].str.fullmatch(pattern, na=True)
            except:
                pass

        if col.lower() == "age":
            invalid_mask |= (df[col] < 0) | (df[col] > 120)

    valid_df = df[~invalid_mask]
    invalid_df = df[invalid_mask]
    return (
        valid_df,
        invalid_df,
        {
            "invalid_rows": len(invalid_df),
            "valid_rows": len(valid_df),
            "total_rows": len(df),
        },
    )


def handle_missing(df, col_threshold=0.5, row_threshold=0.5):
    df = df.copy()

    cols = [c for c in df.columns if c not in SYSTEM_COLUMNS]

    col_missing_ratio = df[cols].isnull().mean()
    cols_to_drop = col_missing_ratio[col_missing_ratio > col_threshold].index.tolist()

    df_reduced = df.drop(columns=cols_to_drop)

    remaining_cols = [c for c in df_reduced.columns if c not in SYSTEM_COLUMNS]

    row_missing_ratio = df_reduced[remaining_cols].isnull().mean(axis=1)

    rows_to_keep_mask = row_missing_ratio <= row_threshold
    df_clean = df_reduced[rows_to_keep_mask]
    df_missing = df_reduced[~rows_to_keep_mask]

    return (
        df_clean,
        df_missing,
        {
            "dropped_columns": cols_to_drop,
            "num_dropped_columns": len(cols_to_drop),
            "rows_with_missing": int((~rows_to_keep_mask).sum()),
            "rows_clean": int(rows_to_keep_mask.sum()),
            "col_threshold": col_threshold,
            "row_threshold": row_threshold,
        },
    )


def is_date_column(col_name):
    norm = normalize(col_name)
    return any(k in norm for k in ["date", "time", "dob", "birth", "day"])


def convert_types(df):
    df = df.copy()
    report = []

    for col in df.columns:
        if df[col].dtype != "object":
            continue

        series = df[col].dropna().astype(str)

        numeric_ratio = series.str.match(r"^\d+$").mean()

        if numeric_ratio >= 0.8:
            df[col] = pd.to_numeric(df[col], errors="coerce")
            report.append({"column": col, "type": "numeric"})
            continue

        if is_date_column(col):
            parsed = pd.to_datetime(df[col], errors="coerce")
            if parsed.notna().mean() >= 0.8:
                df[col] = parsed
                report.append({"column": col, "type": "datetime"})

    return df, report


def normalize_gender(val):
    if pd.isna(val):
        return val

    v = str(val).strip().lower()
    if v in {"m", "male", "man"}:
        return "Male"
    if v in {"f", "female", "woman"}:
        return "Female"
    return "Other"


def normalize_boolean(val):
    if pd.isna(val):
        return val

    v = str(val).strip().lower()
    if v in {"yes", "true", "1"}:
        return True
    if v in {"no", "false", "0"}:
        return False
    return val


def normalize_text(val):
    if pd.isna(val):
        return val
    return str(val).strip().title()


def normalize_values(df):
    df = df.copy()

    if "Gender" in df.columns:
        df["Gender"] = df["Gender"].apply(normalize_gender)

    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].apply(normalize_boolean)

        # ONLY normalize text if still string
        df[col] = df[col].apply(
            lambda x: normalize_text(x) if isinstance(x, str) else x
        )

    return df


def run_step2(df):
    reports = {}

    df, reports["duplicates"] = handle_duplicates(df)

    df_valid, df_invalid, reports["validation"] = validate_inputs(df)

    df_clean, df_missing, reports["missing"] = handle_missing(df_valid)

    df_clean, reports["type_conversion"] = convert_types(df_clean)

    df_clean = normalize_values(df_clean)

    if len(df_clean) > 0:
        df_clean = score_data_quality(df_clean)
        reports["quality"] = summarize_quality(df_clean)
    else:
        reports["quality"] = {
            "avg_score": 0,
            "min_score": 0,
            "max_score": 0,
            "row_count": 0,
            "distribution_pct": {},
        }

    reports["quality"] = summarize_quality(df_clean)

    return df_clean, df_invalid, df_missing, reports
