import os
import pandas as pd

csv_dir = "./data/csv"   # change if needed

for f in sorted(os.listdir(csv_dir)):
    if f.lower().endswith(".csv"):
        path = os.path.join(csv_dir, f)
        cols = pd.read_csv(path, nrows=0).columns.tolist()
        print(f"\n=== {f} ===")
        print(", ".join(cols))