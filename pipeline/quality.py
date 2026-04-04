def score_data_quality(df):
    df = df.copy()

    scores = []

    for _, row in df.iterrows():
        score = 1.0

        missing_ratio = row.isnull().sum() / len(row)
        score -= missing_ratio * 0.5

        empty_count = sum(
            1 for val in row if isinstance(val, str) and val.strip() == ""
        )
        score -= empty_count * 0.05

        score = max(score, 0)
        scores.append(score)

    df["QualityScore"] = scores

    return df


def summarize_quality(df):
    scores = df["QualityScore"]
    total = len(scores)

    def pct(x):
        return round(x * 100, 2)

    summary = {
        "avg_score": round(scores.mean(), 3),
        "min_score": round(scores.min(), 3),
        "max_score": round(scores.max(), 3),
        "row_count": total,
        "distribution_pct": {
            "excellent (0.9-1.0)": pct((scores >= 0.9).mean()),
            "good (0.7-0.9)": pct(((scores >= 0.7) & (scores < 0.9)).mean()),
            "fair (0.5-0.7)": pct(((scores >= 0.5) & (scores < 0.7)).mean()),
            "poor (<0.5)": pct((scores < 0.5).mean()),
        },
    }

    return summary
