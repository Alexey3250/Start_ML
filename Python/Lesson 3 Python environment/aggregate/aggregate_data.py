import pandas as pd

all_data = [1.7, 0.5, None]
data_sum = pd.Series(all_data).sum(skipna=None)
print(data_sum)