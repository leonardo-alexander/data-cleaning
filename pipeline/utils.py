import re


def normalize(col):
    return re.sub(r"[^a-z]", "", col.lower())
