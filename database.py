"""
I want a function that takes
1. a config file of
a. the filters like where and what not
b. the group by these values and it will return a haskell tranposed version without aggregating. basically list of lists. default is to retour an outer singleton list
c. the end user paases in a map an aggregate function on each tranpose if they'd like. identity can be default if they dont want to aggregate
d.
Returns a list of list of values.
But what if a value is also a list for example I want to group by learning rate but then I want to observe
test loss of entire trajectory? I should be able to

"""

import wandb
import polars as pl

api = wandb.Api()
artifact = api.artifact("wlp9800-new-york-university/oho_exps/run-cpm58oj8-history:v0")
download_path = artifact.download(root="./custom_dir", path_prefix="0000.parquet")
df = pl.read_parquet(f"{download_path}/0000.parquet")
print(df)
