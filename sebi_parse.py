from bs4 import BeautifulSoup
import pandas as pd
import csv
from pathlib import Path
import json

with open("pmrid.json", "r") as file:
    pmr_ids = json.load(file)
    id_to_name = {pmr_id.split("@@")[0]: pmr_id.split("@@")[-1] for pmr_id in pmr_ids}


def process_html_tables(html_content, pmr_id, date):
    try:
        dfs = pd.read_html(html_content)
    except Exception as e:
        print(f"Error parsing tables for pmr_id {pmr_id}, date {date}: {e}")
        return
    if len(dfs) == 0:
        return

    for idx, df in enumerate(dfs):
        if idx == 0:
            # set second level of index as values and first level as columns

            columns = df.columns[0]
            values = df.columns[1]
            # remove unnamed from values
            values = ["" if val.startswith("Unnamed") else val for val in values]
            data = dict(zip(columns, values))
            # df from dict
            df = pd.DataFrame([data])
        elif idx == 1:
            # parse a multi level header index and combine the header with the first row if the value is different
            columns = []
            for col in df.columns:
                if col[0].startswith("Unnamed"):
                    columns.append(col[1])
                elif col[0] == col[1]:
                    columns.append(col[0])
                else:
                    columns.append(f"{col[0]} / {col[1]}")
            df.columns = columns

            df.insert(0, "pmr_id", pmr_id)
            df.insert(1, "pmr_name", id_to_name.get(pmr_id, ""))
            # df.to_csv("table_1.csv", index=False)
        elif idx == 2:
            # drop first level index
            df.columns = df.columns.droplevel()
            columns = []
            for col in df.columns:
                if col[1].startswith("Unnamed"):
                    columns.append(col[0])
                elif col[0] == col[1]:
                    columns.append(col[0])
                else:
                    columns.append(f"{col[0]} / {col[1]}")
            df.columns = columns
            df.insert(0, "pmr_id", pmr_id)
            df.insert(1, "pmr_name", id_to_name.get(pmr_id, ""))
        else:
            continue
        df.insert(2, "date", date)
        df.to_csv(f"csvdata/{pmr_id}_{date}_table_{idx}.csv", index=False)


src_dir = "data/"
for html_file in Path(src_dir).glob("**/*.html"):
    pmr_id = html_file.stem.split("_")[1]
    year = html_file.parent.parent.name
    month = html_file.parent.name
    date = f"{year}-{month}"
    with open(html_file, "r") as file:

        html_content = file.read()
        process_html_tables(html_content, pmr_id, date)
