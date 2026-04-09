def score_data_quality(df):
    df = df.copy()
    scores = []

    for _, row in df.iterrows():
        score = 1.0
        total_cols = len(row)

        # Missing values penalty (stronger)
        missing_ratio = row.isnull().sum() / total_cols
        score -= missing_ratio * 0.7

        # Empty string penalty (scaled properly)
        empty_count = sum(
            1 for val in row if isinstance(val, str) and val.strip() == ""
        )
        empty_ratio = empty_count / total_cols
        score -= empty_ratio * 0.3

        # Clamp
        score = max(min(score, 1), 0)
        scores.append(score)

    df["QualityScore"] = scores
    return df


import numpy as np


def summarize_quality(df):
    scores = df["QualityScore"]
    total = len(scores)

    def pct(x):
        return round(x * 100, 2)

    avg_score = scores.mean()
    std_dev = scores.std()

    # Distribution
    excellent = (scores >= 0.9).mean()
    good = ((scores >= 0.7) & (scores < 0.9)).mean()
    fair = ((scores >= 0.5) & (scores < 0.7)).mean()
    poor = (scores < 0.5).mean()

    distribution = {
        "excellent (0.9-1.0)": pct(excellent),
        "good (0.7-0.9)": pct(good),
        "fair (0.5-0.7)": pct(fair),
        "poor (<0.5)": pct(poor),
    }

    # --- HYBRID SCORE ---
    # Consistency factor (penalize no variation OR high noise)
    consistency = 1 - std_dev

    # Diversity factor (penalize overly uniform distribution)
    max_bucket = max(excellent, good, fair, poor)
    expected_max = 0.7
    diversity_factor = max(0, 1 - (max_bucket - expected_max))

    # Final hybrid score
    final_score = avg_score * consistency * diversity_factor

    summary = {
        "avg_score": round(avg_score, 3),
        "min_score": round(scores.min(), 3),
        "max_score": round(scores.max(), 3),
        "std_dev": round(std_dev, 3),
        "row_count": total,
        "distribution_pct": distribution,
        "final_quality_score": round(final_score, 3),
    }

    return summary
