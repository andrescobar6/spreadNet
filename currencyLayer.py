import os
import requests
import pandas as pd
from dotenv import load_dotenv
from google.cloud import storage

load_dotenv("currencyLayer.env")
access_key = os.getenv('API_CURRENCYLAYER')

#_____

url = "http://apilayer.net/api/live"

headers = {
    "access_key": access_key,
    "source": "USD",
    "currencies ": "ARS,CLP,PEN,COP",
    "format": 1
}
response = requests.get(url, params=headers)
response = response.json()

#_____

response=pd.DataFrame(response)
response["MARKET"]=list(response.index.values)
response.reset_index(inplace=True,drop=True)
response = response[["MARKET","quotes"]]
response.columns=["MARKET","QUOTES"]
response=response.loc[response.MARKET.isin(["USDARS","USDCLP","USDPEN","USDCOP"])]
response.reset_index(inplace=True,drop=True)
response=response.to_dict()

#_____

os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="gcp_json.json"
storage_client = storage.Client()
bucket = storage_client.get_bucket(bucket_or_name="marketmaker")

blob=bucket.blob("fiatQuotes.txt")
blob.upload_from_string(data=str(response))