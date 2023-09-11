
from multiprocessing.pool import ThreadPool as Pool
import pandas as pd
import openai
import math
import random
import numpy as np
import json
APIKEY = "THIS IS SUPERSECRET"
# Read the XLSX file
file_path = r"yourfullyqualifiedfilepath.xlsx"

def RunParallel(List, pool_size=10):
    def worker(func):
        func()
    pool = Pool(pool_size)
    for func in List:
        pool.apply_async(worker, (func,))
    pool.close()
    pool.join()
def Extract_Company_Info(file_path=None, sheet_name=None):
    df = pd.read_excel(file_path, engine='openpyxl', sheet_name=sheet_name)
    openai.api_key = APIKEY

    def extract_data(text):
        # Construct the prompt for the API
        import json

        name = ""
        membership = ""
        join_date = ""
        company_name = ""
        street_address = ""
        city = ""
        state = ""
        zip_code = ""
        phone_number = ""
        email = ""

        prompt = f"""Extract the following information from the given text and return it in a JSON format: 
        - Name
        - Membership
        - Join date
        - Company name
        - Street address
        - City
        - State
        - Zip code
        - Phone number
        - Email

    Text: {text}

    Output: {{
        "name": "{name}" if name else "",
        "membership": "{membership}" if membership else "",
        "join date": "{join_date}" if join_date else "",
        "company name": "{company_name}" if company_name else "",
        "street address": "{street_address}" if street_address else "",
        "city": "{city}" if city else "",
        "state": "{state}" if state else "",
        "zip code": "{zip_code}" if zip_code else "",
        "phone number": "{phone_number}" if phone_number else "",
        "email": "{email}" if email else ""
    }}
    """

        # Call the API
        response = openai.Completion.create(
            engine="text-davinci-002", prompt=prompt, max_tokens=150, n=1, temperature=0.0,)
        # Extract the generated text
        return response.choices[0].text.strip()

    def process_data_in_batches(text_strings, batch_size):
        num_batches = math.ceil(len(text_strings) / batch_size)
        extracted_data = []
        for i in range(num_batches):
            start_index = i * batch_size
            end_index = (i + 1) * batch_size
            batch = text_strings[start_index:end_index]
            # Extract data for each text string in the batch
            extracted_batch = [extract_data(text) for text in batch]
            extracted_data.extend(extracted_batch)
        return extracted_data
    # Process the data in batches
    batch_size = 10  # Adjust the batch size according to your needs and rate limits
    try:
        # df = df.sample(frac=0.010)
        print(df.shape)
        # Convert the data to a list of text strings
        text_strings = df['source'].tolist()
        extracted_data = process_data_in_batches(text_strings, batch_size)
        return extracted_data
    except Exception as e:
        print(e)



fp = r"filepath.xlsx"
dfd = pd.read_excel(fp, sheet_name=None)
with open("process_log.txt", "r") as f:
    log = f.read().split("\n")
    log = [i for i in log if i.strip() != ""]
    print(log)
count=0
for tab in dfd.keys():
    print("sheet name chosen:", tab)
    if tab in (log):
        print("already exists. terminating loop...")
        print("="*50)
        continue
    else:
        print("moving onto process...")
        extracted_data = Extract_Company_Info(file_path=fp, sheet_name=tab)
        if len(extracted_data) > 0:
            with open("process_log.txt","a") as f: f.write(f"{tab}\n")
            save_data="\n==================================================\n".join(extracted_data)
            destination=rf"ExtractResulst_{tab}.txt"
            with open(destination,"w") as f:
                f.write(save_data)
            count+=1
    print("="*50)





