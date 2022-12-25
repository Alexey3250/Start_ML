import pandas as pd


df = pd.DataFrame([
  ["g", "g0"],
  ["g", "g1"],
  ["g", "g2"],
  ["g", "g3"],
  ["h", "h0"],
  ["h", "h1"]
], columns=["A", "B"])
print(df.groupby("A").nth[1:-1])