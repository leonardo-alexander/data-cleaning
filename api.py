from fastapi import FastAPI, UploadFile, File, Query
from fastapi.responses import StreamingResponse
import pandas as pd
import io
import numpy as np
import time

from pipeline.step1 import run_step1
from pipeline.step2 import run_step2

app = FastAPI(title="Data Cleaning Pipeline API")


# Clean JSON
def clean_for_json(data):
    if isinstance(data, (np.integer,)):
        return int(data)

    if isinstance(data, (np.floating,)):
        if np.isnan(data) or np.isinf(data):
            return None
        return float(data)

    if isinstance(data, float):
        if np.isnan(data) or np.isinf(data):
            return None
        return data

    if isinstance(data, dict):
        return {k: clean_for_json(v) for k, v in data.items()}

    if isinstance(data, list):
        return [clean_for_json(v) for v in data]

    return data


# CSV Loader
async def load_csv(file: UploadFile):
    if not file.filename.endswith(".csv"):
        return None, {"error": "Only CSV files are supported"}

    content = await file.read()

    try:
        df = pd.read_csv(io.BytesIO(content))
    except Exception:
        return None, {"error": "Invalid CSV file"}

    if df.empty:
        return None, {"error": "Uploaded file is empty"}

    if len(df) > 100_000:
        return None, {"error": "Dataset too large (max 100k rows)"}

    return df, None


# MAIN PROCESS ENDPOINT
@app.post("/process")
async def process_file(
    file: UploadFile = File(...),
    mode: str = Query("step1", enum=["step1", "full"]),
):
    start_time = time.time()

    df, error = await load_csv(file)
    if error:
        return error

    # --- STEP 1 ---
    if mode == "step1":
        df_clean, report = run_step1(df)

        response = {
            "mode": "step1",
            "report": report,
            "preview": df_clean.head(5).to_dict(orient="records"),
            "data": df_clean.to_dict(orient="records")
        }

    # --- FULL ---
    elif mode == "full":
        df_step1, report1 = run_step1(df)
        df_clean, df_invalid, df_missing, report2 = run_step2(df_step1)

        response = {
            "mode": "full",
            "report": {"step1": report1, "step2": report2},
            "preview": df_clean.head(5).to_dict(orient="records"),
            "invalid_rows": int(len(df_invalid)),
            "missing_rows": int(len(df_missing)),
            "data": df_clean.to_dict(orient="records")
        }

    else:
        return {"error": "Invalid mode"}

    # --- Clean NaN ---
    response = clean_for_json(response)

    # --- Processing time ---
    response["processing_time_ms"] = int((time.time() - start_time) * 1000)

    return response


# DOWNLOAD ENDPOINT
@app.post("/process/download")
async def process_and_download(
    file: UploadFile = File(...),
    mode: str = Query("step1", enum=["step1", "full"]),
):
    df, error = await load_csv(file)
    if error:
        return error

    if mode == "step1":
        df_clean, _ = run_step1(df)

    elif mode == "full":
        df_step1, _ = run_step1(df)
        df_clean, _, _, _ = run_step2(df_step1)

    else:
        return {"error": "Invalid mode"}

    # --- Clean values ---
    df_clean = df_clean.replace([np.nan, np.inf, -np.inf], None)

    # --- Convert to CSV ---
    buffer = io.StringIO()
    df_clean.to_csv(buffer, index=False)
    buffer.seek(0)

    return StreamingResponse(
        buffer,
        media_type="text/csv",
        headers={"Content-Disposition": f"attachment; filename=cleaned_{mode}.csv"},
    )
