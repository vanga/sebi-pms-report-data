# find csv files in csvdata directory
# combine them into a single csv file based on each suffix of table_0 or table_1 or table_2
import csv
import pandas as pd

from pathlib import Path

csvdata_dir = Path("csvdata")

# Get all the csv files in the csvdata directory
csv_files = list(csvdata_dir.glob("*.csv"))

# Create a dictionary to store the dataframes based on the suffix of the file name
dfs = {}
for csv_file in csvdata_dir.glob("*.csv"):
    suffix = csv_file.stem.split("_")[-1]
    if suffix not in dfs:
        dfs[suffix] = pd.DataFrame()
    df = pd.read_csv(csv_file)
    dfs[suffix] = pd.concat([dfs[suffix], df])

df0 = dfs["0"]
df1 = dfs["1"]
df2 = dfs["2"]

# rename Registration Number to pmr_id
df0.rename(
    columns={
        "Registration Number": "pmr_id",
        "No. of clients as on last day of the month": "No. of clients",
        "Total Assets under Management (AUM) as on last day of the month (Amount in INR crores)": "Total AUM(in Cr)",
    },
    inplace=True,
)

df1_value_map = {
    "No. of unique Clients as on last day of the month": "No. of clients",
    "Assets under Management (AUM) as on last day of the month (Amount in INR crores)": "Total AUM(in Cr)",
}
df1["Particulars"] = df1["Particulars"].map(df1_value_map)
df1_pivot = df1.pivot_table(
    index=["pmr_id", "pmr_name", "date"], columns=["Particulars"]
)
# flatten column names based on multi index
df1_pivot.columns = [" / ".join(col).strip() for col in df1_pivot.columns.values]

df2_pivot = df2.pivot_table(
    index=["pmr_id", "pmr_name", "date"], columns=["Type of PMS Service"]
)
# flatten column names based on multi index
df2_pivot.columns = [" / ".join(col).strip() for col in df2_pivot.columns.values]


# Merge the dataframes based on the common columns
merged_df = pd.merge(df0, df1_pivot, on=["pmr_id", "date"], how="outer")
merged_df = pd.merge(merged_df, df2_pivot, on=["pmr_id", "date"], how="outer")

# Save the merged dataframe to a csv file
merged_df.to_csv("merged_data.csv", index=False)
