import requests
import os
from datetime import datetime, timedelta, timezone
import json
import time
import shutil
import pandas as pd

end_date = datetime(2024, 8, 31)
pmrd_path = "pmrid.json"

df = pd.read_csv("merged_data.csv")


def fetch_and_save_pages():
    url = "https://www.sebi.gov.in/sebiweb/other/OtherAction.do?doPmr=yes"

    headers = {
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language": "en-US,en;q=0.9,pt;q=0.8",
        "Cache-Control": "max-age=0",
        "Connection": "keep-alive",
        "Content-Type": "application/x-www-form-urlencoded",
        "DNT": "1",
        "Origin": "https://www.sebi.gov.in",
        "Referer": "https://www.sebi.gov.in/sebiweb/other/OtherAction.do?doPmr=yes",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Upgrade-Insecure-Requests": "1",
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/127.0.0.0 Safari/537.36",
        "sec-ch-ua": '"Not)A;Brand";v="99", "Google Chrome";v="127", "Chromium";v="127"',
        "sec-ch-ua-mobile": "?0",
        "sec-ch-ua-platform": '"macOS"',
    }
    with open(pmrd_path, "r") as file:
        pmr_ids = json.load(file)

    for pmr_id in pmr_ids:
        pmr_short_id = pmr_id.split("@@")[0]
        reg_date = df[df["pmr_id"] == pmr_short_id]["Date of Registration"].values
        if len(reg_date) == 0 or pd.isnull(reg_date[0]):
            reg_date = datetime(year=datetime.now(timezone.utc).year, month=1, day=1)
        else:
            reg_date = datetime.strptime(reg_date[0], "%Y-%m-%d").replace(day=1)
        current_date = reg_date
        while current_date <= end_date:
            year = current_date.year
            month = current_date.month
            short_id = pmr_short_id
            data = {
                "currdate": "",
                "loginflag": "0",
                "searchValue": "",
                "pmrId": pmr_id,
                "year": str(year),
                "month": str(month),
                "org.apache.struts.taglib.html.TOKEN": "...",
                "loginEmail": "",
                "loginPassword": "",
                "cap_login": "",
                "moduleNo": "-1",
                "moduleId": "",
                "link": "",
                "yourName": "",
                "friendName": "",
                "friendEmail": "",
                "mailmessage": "",
                "cap_email": "",
            }

            try:
                directory = f"data/{year}/{month:02d}"
                os.makedirs(directory, exist_ok=True)
                short_id_path = f"{directory}/pmr_{short_id}.html"
                if os.path.exists(short_id_path):
                    print(f"Skipping: {short_id_path}")
                else:
                    response = requests.post(url, headers=headers, data=data)
                    response.raise_for_status()

                    # Create directory structure

                    # Save the webpage

                    # assert (
                    #     "No Records Found" in response.text
                    #     or "Name of the Portfolio Manager" in response.text
                    # ), f"Error fetching data for pmrId {pmr_id}, year {year}, month {month}"
                    with open(short_id_path, "w", encoding="utf-8") as file:
                        file.write(response.text)

                    print(f"Saved: {short_id_path}")
                    time.sleep(0.2)
            except requests.RequestException as e:
                print(
                    f"Error fetching data for pmrId {pmr_id}, year {year}, month {month}: {e}"
                )

            # Move to the next month
            current_date += timedelta(days=32)
            current_date = current_date.replace(day=1)


fetch_and_save_pages()
