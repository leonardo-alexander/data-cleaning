import numpy as np


def score_data_quality(df):
    df = df.copy()
    scores = []

    for _, row in df.iterrows():
        score = 1.0
        total_cols = len(row)

        # Missing values penalty
        missing_ratio = row.isnull().sum() / total_cols
        score -= missing_ratio * 0.7

        # Empty string penalty (scaled)
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

    # Consistency
    consistency = 1 - std_dev

    # Diversity (entropy-based)
    probs = np.array([excellent, good, fair, poor])
    probs = probs[probs > 0]

    if len(probs) > 1:
        entropy = -np.sum(probs * np.log(probs))
        max_entropy = np.log(4)
        diversity_factor = entropy / max_entropy
    else:
        diversity_factor = 1.0

    # Final score (weighted)
    final_score = avg_score * 0.6 + consistency * 0.25 + diversity_factor * 0.15

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
