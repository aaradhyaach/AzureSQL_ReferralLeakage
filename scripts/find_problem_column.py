import pandas as pd
df = pd.read_csv("./data/csv/claims.csv", dtype=str).fillna("")
maxlens = {c: df[c].map(len).max() for c in df.columns}
for c, m in sorted(maxlens.items(), key=lambda x: -x[1]):
    print(f"{c}: {m}")