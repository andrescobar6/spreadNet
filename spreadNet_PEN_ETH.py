#_____MARKET
MONEY="PEN"
CRYPT="ETH"

#_____CORREOS
owners_warning=["andrescobar6@gmail.com"]

#_____VARIABLES DE CONTROL
sleepApis=2
sleepError=2
priceDistance=5
marketDecimals=3
sleepErrorApis=10
tradeProportion=0.8
minVolumeTrade=0.001
utilityMarginThreshold=0.001
maxTradingVolumeProportion=0.8

#_____LIBRERÍAS
import os
import ast
import hmac
import json
import math
import time
import surbtc
import base64
import smtplib
import requests
import pandas as pd
import requests.auth
from pandas_gbq import gbq
from dotenv import load_dotenv
from google.cloud import storage
from google.cloud import bigquery
from email.mime.text import MIMEText
from trading_api_wrappers import Buda
from email.mime.multipart import MIMEMultipart

#_____FUNCIONES GENERALES_____#

# FUNCIÓN QUE REDONDEA FLOAT HACIA ABAJO SEGÚN DECIMALES
def round_decimals_down(number:float, decimals:int=2):
    """
    Returns a value rounded down to a specific number of decimal places.
    """
    if not isinstance(decimals, int):
        raise TypeError("decimal places must be an integer")
    elif decimals < 0:
        raise ValueError("decimal places has to be 0 or more")
    elif decimals == 0:
        return math.floor(number)

    factor = 10 ** decimals
    return math.floor(number * factor) / factor

# FUNCIÓN QUE REDONDEA FLOAT HACIA ARRIBA SEGÚN DECIMALES
def round_decimals_up(number:float, decimals:int=2):
    """
    Returns a value rounded down to a specific number of decimal places.
    """
    if not isinstance(decimals, int):
        raise TypeError("decimal places must be an integer")
    elif decimals < 0:
        raise ValueError("decimal places has to be 0 or more")
    elif decimals == 0:
        return math.ceil(number)

    factor = 10 ** decimals
    return math.ceil(number * factor) / factor

# FUNCIÓN QUE LLAMA A LA CLASE BUDA
class BudaHMACAuth(requests.auth.AuthBase):
    """Adjunta la autenticación HMAC de Buda al objeto Request."""

    def __init__(self, api_key: str, secret: str):
        self.api_key = api_key
        self.secret = secret

    def get_nonce(self) -> str:
        # 1. Generar un nonce (timestamp en microsegundos)
        return str(int(time.time() * 1e6))

    def sign(self, r, nonce: str) -> str:
        # 2. Preparar string para firmar
        components = [r.method, r.path_url]
        if r.body:
            encoded_body = base64.b64encode(r.body).decode()
            components.append(encoded_body)
        components.append(nonce)
        msg = ' '.join(components)
        # 3. Obtener la firma
        h = hmac.new(key=self.secret.encode(),
                        msg=msg.encode(),
                        digestmod='sha384')
        signature = h.hexdigest()
        return signature

    def __call__(self, r):
        nonce = self.get_nonce()
        signature = self.sign(r, nonce)
        # 4. Adjuntar API-KEY, nonce y firma al header del request
        r.headers['X-SBTC-APIKEY'] = self.api_key
        r.headers['X-SBTC-NONCE'] = nonce
        r.headers['X-SBTC-SIGNATURE'] = signature
        return r

# FUNCIÓN QUE CREA CONEXIÓN CON BUDA
def create_connection_buda():
    global API_KEY
    global API_SECRET
    return Buda.Auth(API_KEY, API_SECRET)

# FUNCIÓN QUE ENVÍA ALERTA POR CORREO ELECTRÓNICO
def enviar_alerta(subject, msg, owners):

    global me
    global password

    #___Create email object
    email = MIMEMultipart('alternative')

    #___Set parameters
    email['Subject'] = subject
    email['From'] = me
    body = MIMEText('<html><body><p>{}</p></body></html>'.format(msg), 'html')

    #___Attach body
    email.attach(body)

    #___Create connection
    with smtplib.SMTP_SSL('smtp.gmail.com') as connection:

        connection.login(me, password)

        #___Send email
        for owner in owners:
            email['To'] = owner
            connection.sendmail(me, owner, email.as_string())

#_____FUNCIONES ESPECÍFICAS_____#

# ACTUALIA BALANCE CRYPTO
def getCRYinAccount():
    
    global CRYPT
    global client
    global sleepApis
    global sleepError
    global balanceCRY

    time.sleep(sleepApis)

    while True:
        try:   
            balance = client.balance(CRYPT)
            balanceCRY = balance.amount.amount
            break
        except:
            print("[ERROR]: getCRYinAccount()")
            time.sleep(sleepError)

    return balance.amount.amount

# ACTUALIA BALANCE FIAT
def getMONinAccount():
    
    global MONEY
    global client
    global sleepApis
    global sleepError
    global balanceMON

    time.sleep(sleepApis)

    while True:
        try:
            balance = client.balance(MONEY)
            balanceMON = balance.amount.amount    
            break
        except:
            print("[ERROR]: getMONinAccount()")
            time.sleep(sleepError)

    return balance.amount.amount

# ACTUALIZA TAZA DE CAMBIO DE MERCADOS A USD
def getFiatUsdQuote(fiat):
    
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]='gcp_json.json'
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_or_name="marketmaker")
    fiatQuotes=bucket.get_blob(blob_name="fiatQuotes.txt")
    fiatQuotes=fiatQuotes.download_as_string()
    fiatQuotes=pd.DataFrame(ast.literal_eval(fiatQuotes.decode("utf-8")))
    return float(fiatQuotes.loc[fiatQuotes.MARKET == "USD"+fiat.upper()].QUOTES.values[0])

# SHUTDOWN MARKET
def shutDownMarket(CRYPT,MONEY):

    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]='gcp_json.json'
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_or_name="marketmaker")
    marketControl=bucket.get_blob(blob_name="marketControl.txt")
    marketControl=marketControl.download_as_string()
    marketControl=pd.DataFrame(ast.literal_eval(marketControl.decode("utf-8")))

    index=marketControl.loc[marketControl.MARKET==CRYPT.upper()+"_"+MONEY.upper()].index.values[0]
    marketControl.at[index,"ON"]=0

    marketControl=marketControl.to_dict()

    blob=bucket.blob("marketControl.txt")
    blob.upload_from_string(data=str(marketControl))

# ON/OFF MARKET
def getOnOffMarket(CRYPT,MONEY):
    
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]='gcp_json.json'
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_or_name="marketmaker")
    marketControl=bucket.get_blob(blob_name="marketControl.txt")
    marketControl=marketControl.download_as_string()
    marketControl=pd.DataFrame(ast.literal_eval(marketControl.decode("utf-8")))
    return int(marketControl.loc[marketControl.MARKET == CRYPT.upper()+"_"+MONEY.upper()].ON.values[0])

# ACTUALIZA EL BALANCE DE ASKS Y BIDS EJECUTADOS
def updatePast_Asks_Bids():
  
    global MONEY
    global CRYPT
    global pastAsks
    global pastBids
    global sleepError
    global database_past_asks_bids

    pastAsks=0.0
    pastBids=0.0

    while True:
        try:
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"]='gcp_json.json'
            database_past_asks_bids=gbq.read_gbq("SELECT * FROM [dogwood-terra-308100:spreadNet."+CRYPT+"_"+MONEY+"] ORDER BY CREATED_AT DESC",project_id="dogwood-terra-308100",dialect="legacy")
            break
        except:
            print("[ERROR]: updatePast_Asks_Bids()")
            time.sleep(sleepError)

    #_____SI HAY DATOS EN LA BASE DE DATOS
    if len(database_past_asks_bids)>0:

        #_____FORMAT DATABASE
        database_past_asks_bids.reset_index(inplace=True,drop=True,level=0)

        database_past_asks_bids.ID=database_past_asks_bids.ID.astype(str)
        database_past_asks_bids.ACCOUNT_ID=database_past_asks_bids.ACCOUNT_ID.astype(str)
        database_past_asks_bids.AMOUNT=database_past_asks_bids.AMOUNT.astype(float)
        database_past_asks_bids.CREATED_AT= pd.to_datetime(database_past_asks_bids.CREATED_AT)
        database_past_asks_bids.FEE_CURRENCY=database_past_asks_bids.FEE_CURRENCY.astype(str)
        database_past_asks_bids.LIMIT=database_past_asks_bids.LIMIT.astype(float)
        database_past_asks_bids.MARKET_ID=database_past_asks_bids.MARKET_ID.astype(str)
        database_past_asks_bids.ORIGINAL_AMOUNT=database_past_asks_bids.ORIGINAL_AMOUNT.astype(float)
        database_past_asks_bids.PAID_FEE=database_past_asks_bids.PAID_FEE.astype(float)
        database_past_asks_bids.PRICE_TYPE=database_past_asks_bids.PRICE_TYPE.astype(str)
        database_past_asks_bids.STATE=database_past_asks_bids.STATE.astype(str)
        database_past_asks_bids.TOTAL_EXCHANGED=database_past_asks_bids.TOTAL_EXCHANGED.astype(float)
        database_past_asks_bids.TRADED_AMOUNT=database_past_asks_bids.TRADED_AMOUNT.astype(float)
        database_past_asks_bids.TYPE=database_past_asks_bids.TYPE.astype(str)
        database_past_asks_bids.MY_CRYPTO=database_past_asks_bids.MY_CRYPTO.astype(float)
        database_past_asks_bids.MY_FIAT=database_past_asks_bids.MY_FIAT.astype(float)
        database_past_asks_bids.MY_TRM=database_past_asks_bids.MY_TRM.astype(float)
        database_past_asks_bids.MY_CRYPTO_IN_FIAT=database_past_asks_bids.MY_CRYPTO_IN_FIAT.astype(float)
        database_past_asks_bids.MY_CRYPTO_IN_USD=database_past_asks_bids.MY_CRYPTO_IN_USD.astype(float)
        database_past_asks_bids.MY_FIAT_IN_USD=database_past_asks_bids.MY_FIAT_IN_USD.astype(float)
        database_past_asks_bids.MY_MARKET_USD=database_past_asks_bids.MY_MARKET_USD.astype(float)
        database_past_asks_bids.ORIGINAL_AMOUNT_USD=database_past_asks_bids.ORIGINAL_AMOUNT_USD.astype(float)
        database_past_asks_bids.MY_EXECUTED_AMOUNT_USD=database_past_asks_bids.MY_EXECUTED_AMOUNT_USD.astype(float)
        database_past_asks_bids.MY_OPERATIONAL_UTILITY_FIAT=database_past_asks_bids.MY_OPERATIONAL_UTILITY_FIAT.astype(float)
        database_past_asks_bids.MY_OPERATIONAL_UTILITY_USD=database_past_asks_bids.MY_OPERATIONAL_UTILITY_USD.astype(float)

        #_____BALANCE
        pastAsks=database_past_asks_bids.loc[(database_past_asks_bids['TYPE'] == "Ask") &
                                    (database_past_asks_bids['PRICE_TYPE'] == "limit") &
                                    (database_past_asks_bids["MARKET_ID"]==CRYPT+"-"+MONEY) &
                                    ((database_past_asks_bids['STATE'] == "canceled")|
                                    (database_past_asks_bids['STATE'] == "traded")), 'TRADED_AMOUNT'].sum()

        pastBids=database_past_asks_bids.loc[(database_past_asks_bids['TYPE'] == "Bid") &
                                    (database_past_asks_bids['PRICE_TYPE'] == "limit") &
                                    (database_past_asks_bids["MARKET_ID"]==CRYPT+"-"+MONEY) &
                                    ((database_past_asks_bids['STATE'] == "canceled")|
                                    (database_past_asks_bids['STATE'] == "traded")), 'TRADED_AMOUNT'].sum()

        pastAsks=round_decimals_down(pastAsks,5)
        pastBids=round_decimals_down(pastBids,5)

# ME DICE EL VOLUMEN QUE DEBO PONER
def history_trades():
    
    global MONEY
    global CRYPT
    global client
    global pastAsks
    global pastBids
    global sleepApis
    global sleepError
    global minVolumeTrade
    global maxTradingVolumeProportion

    updatePast_Asks_Bids()

    if abs(pastAsks-pastBids)<minVolumeTrade:
        
        myActualMoney=getMONinAccount()*maxTradingVolumeProportion
        myActualCrypt=getCRYinAccount()*maxTradingVolumeProportion
        recomendedVolume=myActualCrypt

        #_____SIMULAR MARKET ORDER CON EL RECOMENDED VOLUME QUE TENGO

        time.sleep(sleepApis)
        
        while True:
            try:
                sellQuotation=client.quotation_market(amount=myActualMoney,quotation_type="bid_given_value",market_id=CRYPT.lower()+"-"+MONEY.lower())
                sellQuotation=sellQuotation.base_balance_change[0]
                break
            except:
                print("[ERROR]: history_trades()")
                time.sleep(sleepError)

        #_____ 

        if recomendedVolume>sellQuotation:
            recomendedVolume=sellQuotation
                
    else:
        recomendedVolume=abs(pastAsks-pastBids)

    return round_decimals_down(recomendedVolume,5)

# ACTUALIZO BASE DE DATOS + BALANCEO + ACTUALIZAR FIAT + CRYPTO DISPONIBLES
def balancing_Ask_Bid():
    
    global MONEY
    global CRYPT
    global volume
    global pastAsks
    global pastBids
    global askVolume
    global bidVolume
    global owners_warning
    
    volume=history_trades()
    
    if abs(pastAsks-pastBids)<=minVolumeTrade:
        askVolume=volume
        bidVolume=volume
    elif pastAsks<pastBids:
        if (round_decimals_down(pastBids-pastAsks,5)>volume):
            subject = "SpreadNet: WARNING"
            msg = "[[ERROR]]: balancing_Ask_Bid() -> if (round_decimals_down(pastBids-pastAsks,5)>volume)"
            load_dotenv("spreadNet.env")
            owners_warning = os.getenv('owners_warning')
            owners_warning = json.loads(owners_warning)
            enviar_alerta(subject, msg, owners_warning)
        else:
            askVolume=round_decimals_down(pastBids-pastAsks,5) if (pastBids-pastAsks>=minVolumeTrade) else 0.0
            bidVolume=0.0
    elif pastAsks>pastBids:
        if (round_decimals_down(pastAsks-pastBids,5)>volume):
            subject = "SpreadNet: WARNING"
            msg = "[[ERROR]]: balancing_Ask_Bid() -> if (round_decimals_down(pastBids-pastAsks,5)>volume)"
            load_dotenv("spreadNet.env")
            owners_warning = os.getenv('owners_warning')
            owners_warning = json.loads(owners_warning)
            enviar_alerta(subject, msg, owners_warning)
        else:
            bidVolume=round_decimals_down(pastAsks-pastBids,5) if (pastAsks-pastBids>=minVolumeTrade) else 0.0
            askVolume=0.0

# ESCRIBIR BASE DE DATOS DE TRANSACCIONES EN BIGQUERY
def write_buy_sell_prices():

    global MONEY
    global CRYPT
    global theoryBuyExecuted
    global theorySellExecuted

    #_____CREATE DICT
    last_buy_sell_prices_dic = {"theorySellExecuted": theorySellExecuted,
                                "theoryBuyExecuted": theoryBuyExecuted}

    #_____CREAR CONEXIÓN CON GOOGLE CLOUD
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="gcp_json.json"
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_or_name="marketmaker")
    
    #_____UPLOAD FILE
    blob=bucket.blob("last_buy_sell_prices_dic_"+CRYPT+"_"+MONEY+".txt")
    blob.upload_from_string(data=str(last_buy_sell_prices_dic))

# LEER PRECIOS DE EJECUCIÓN PASADOS
def read_buy_sell_prices():
    
    global MONEY
    global CRYPT
    global theorySellExecuted
    global theoryBuyExecuted
    
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="gcp_json.json"
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_or_name="marketmaker")

    last_buy_sell_prices_dic=bucket.get_blob(blob_name="last_buy_sell_prices_dic_"+CRYPT+"_"+MONEY+".txt")
    last_buy_sell_prices_dic=last_buy_sell_prices_dic.download_as_string()
    last_buy_sell_prices_dic=last_buy_sell_prices_dic.decode("UTF-8")
    last_buy_sell_prices_dic=ast.literal_eval(last_buy_sell_prices_dic)

    theoryBuyExecuted=last_buy_sell_prices_dic["theoryBuyExecuted"]
    theorySellExecuted=last_buy_sell_prices_dic["theorySellExecuted"]

# ELIMINA TODAS LAS ORDENES EXISTENTES + ESCRIBO ODERNES EJECUTADAS
def finishThemAll():

    global MONEY
    global CRYPT
    global client
    global API_KEY
    global sleepApis
    global API_SECRET
    global bidOrderId
    global askOrderId
    global sleepError
    global client_surbtc
    global theoryBuyPrice
    global bidOrderDetails
    global askOrderDetails
    global theorySellPrice
    global theoryBuyExecuted
    global theorySellExecuted

    #_____PARAMETERS
    askPendings=0
    bidPendings=0

    askOrderId=None
    askOrderDetails=None

    bidOrderId=None
    bidOrderDetails=None

    time.sleep(sleepApis)
    
    while True:
        try:
            Market = client_surbtc.getMarket(CRYPT+"-"+MONEY)
            data=pd.DataFrame(Market.getPendingOrders())
            break
        except:
            print("[ERROR]: finishThemAll() -> client_surbtc")
            time.sleep(sleepError)
    
    #_____SI TENGO ÓRDENES
    if len(data)>0:
    
        askPendings=len(data.loc[((data.state=="pending") | (data.state=="accepted")) & (data.type=="Ask")])
        bidPendings=len(data.loc[((data.state=="pending") | (data.state=="accepted")) & (data.type=="Bid")])

        #_____SI TENGO ÓRDENES EJECUTADAS PARCIAL O TOTALMENTE
        if (askPendings>0) or (bidPendings>0):

            askIdList=list(data.loc[((data.state=="pending") | (data.state=="accepted")) & (data.type=="Ask")].id.values)
            bidIdList=list(data.loc[((data.state=="pending") | (data.state=="accepted")) & (data.type=="Bid")].id.values)

            #_____SI TENGO ASKS PENDIENTES
            if len(askIdList)>0:

                for i in askIdList:
                    
                    time.sleep(sleepApis)
                    #_____ACTUALIZAR DETALLES DE ÓRDENES
                    while True:
                        try:
                            askOrderDetailsFinish = client.order_details(i)
                            break
                        except:
                            print("[ERROR]: finishThemAll() -> askOrderDetailsFinish = client.order_details(i)")
                            time.sleep(sleepError)

                    #_____SI EJECUTÉ PARCIAL O TOTALEMTNE LA ÓRDEN -> GUARDAR PRECIOS
                    if (askOrderDetailsFinish.traded_amount.amount > 0.0):
                        theorySellExecuted=theorySellPrice
                        write_buy_sell_prices()
                        
                    time.sleep(sleepApis)

                    #_____ITERAR HASTA QUE SE CANCELE LA ORDEN
                    while True:
                        try:
                            client.cancel_order(i)
                            break
                        except:
                            print("[ERROR]: finishThemAll() -> client.cancel_order(i)")
                            time.sleep(sleepError)

                    #_____ITERAR HASTA QUE SE ACTUALICE EL ESTADO DE LA ORDEN
                    while(True):

                        time.sleep(sleepApis)

                        while True:
                            try:
                                askOrderDetailsFinish = client.order_details(i)
                                break
                            except:
                                print("[ERROR]: finishThemAll() -> askOrderDetailsFinish = client.order_details(i)(1)")
                                time.sleep(sleepError)

                        #_____SI LA ORDEN YA SE ENCUENTRA CANCELADA -> SALIR DEL LOOP
                        if (askOrderDetailsFinish.state=="canceled") or (askOrderDetailsFinish.state=="traded"):
                            break
                        else:
                            time.sleep(sleepApis)
                            while True:
                                try:
                                    client.cancel_order(i)
                                    break
                                except:
                                    print("[ERROR]: finishThemAll() -> askOrderDetailsFinish = client.cancel_order(i)(2)")
                                    time.sleep(sleepError)

                    #_____SI EJECUTÉ PARCIAL O TOTALEMTNE LA ÓRDEN -> GUARDAR PRECIOS
                    if (askOrderDetailsFinish.traded_amount.amount > 0.0):
                    
                        #_____CREAR CONEXIÓN CON BASE DE DATOS EN BIGQUERY
                        os.environ["GOOGLE_APPLICATION_CREDENTIALS"]='gcp_json.json'
                        bigquery_client=bigquery.Client(project="dogwood-terra-308100")
                        dataset_ref=bigquery_client.dataset("spreadNet")
                        table_ref=dataset_ref.table(CRYPT+"_"+MONEY)
                        table=dataset_ref.table(CRYPT+"_"+MONEY)
                        table=bigquery.Table(table)

                        #_____COLUMNAS
                        columns=['ID','UUID','MARKET_ID','ACCOUNT_ID','TYPE','STATE','CREATED_AT','FEE_CURRENCY','PRICE_TYPE','SOURCE','LIMIT','AMOUNT','ORIGINAL_AMOUNT','TRADED_AMOUNT','TOTAL_EXCHANGED','PAID_FEE']

                        #_____CREAR BASE DE DATOS PARA REGISTRO DE ORDEN
                        append_order_dataframe=pd.DataFrame(askOrderDetailsFinish.json).head(1)
                        append_order_dataframe.columns=columns

                        #_____QUITAR COLUMNAS INNECESARIAS
                        append_order_dataframe=append_order_dataframe[["ID","ACCOUNT_ID","AMOUNT","CREATED_AT","FEE_CURRENCY","LIMIT","MARKET_ID","ORIGINAL_AMOUNT","PAID_FEE","PRICE_TYPE","STATE","TOTAL_EXCHANGED","TRADED_AMOUNT","TYPE"]]

                        #_____DAR FORMATO A COLUMNAS
                        append_order_dataframe.ID=append_order_dataframe.ID.astype(str)
                        append_order_dataframe.ACCOUNT_ID=append_order_dataframe.ACCOUNT_ID.astype(str)
                        append_order_dataframe.AMOUNT=append_order_dataframe.AMOUNT.astype(float)
                        append_order_dataframe["CREATED_AT"]=pd.to_datetime(append_order_dataframe["CREATED_AT"])
                        append_order_dataframe.FEE_CURRENCY=append_order_dataframe.FEE_CURRENCY.astype(str)
                        append_order_dataframe.LIMIT=append_order_dataframe.LIMIT.astype(float)
                        append_order_dataframe.MARKET_ID=append_order_dataframe.MARKET_ID.astype(str)
                        append_order_dataframe.ORIGINAL_AMOUNT=append_order_dataframe.ORIGINAL_AMOUNT.astype(float)
                        append_order_dataframe.PAID_FEE=append_order_dataframe.PAID_FEE.astype(float)
                        append_order_dataframe.PRICE_TYPE=append_order_dataframe.PRICE_TYPE.astype(str)
                        append_order_dataframe.STATE=append_order_dataframe.STATE.astype(str)
                        append_order_dataframe.TOTAL_EXCHANGED=append_order_dataframe.TOTAL_EXCHANGED.astype(float)
                        append_order_dataframe.TRADED_AMOUNT=append_order_dataframe.TRADED_AMOUNT.astype(float)
                        append_order_dataframe.TYPE=append_order_dataframe.TYPE.astype(str)

                        #_____AGREGAR COLUMNAS FALTANTES
                        append_order_dataframe.at[0,"MY_CRYPTO"]=getCRYinAccount()
                        append_order_dataframe.at[0,"MY_FIAT"]=getMONinAccount()
                        append_order_dataframe.at[0,"MY_TRM"]=getFiatUsdQuote(MONEY)
                        append_order_dataframe.at[0,"MY_CRYPTO_IN_FIAT"]=append_order_dataframe.at[0,"MY_CRYPTO"]*append_order_dataframe.at[0,"LIMIT"]
                        append_order_dataframe.at[0,"MY_CRYPTO_IN_USD"]=append_order_dataframe.at[0,"MY_CRYPTO_IN_FIAT"]/append_order_dataframe.at[0,"MY_TRM"]
                        append_order_dataframe.at[0,"MY_FIAT_IN_USD"]=append_order_dataframe.at[0,"MY_FIAT"]/append_order_dataframe.at[0,"MY_TRM"]
                        append_order_dataframe.at[0,"MY_MARKET_USD"]=append_order_dataframe.at[0,"MY_CRYPTO_IN_USD"]+append_order_dataframe.at[0,"MY_FIAT_IN_USD"]
                        append_order_dataframe.at[0,"ORIGINAL_AMOUNT_USD"]=(append_order_dataframe.at[0,"ORIGINAL_AMOUNT"]*append_order_dataframe.at[0,"LIMIT"])/append_order_dataframe.at[0,"MY_TRM"]
                        append_order_dataframe.at[0,"MY_EXECUTED_AMOUNT_USD"]=append_order_dataframe.at[0,"TOTAL_EXCHANGED"]/append_order_dataframe.at[0,"MY_TRM"]
                        if append_order_dataframe.at[0,"TYPE"]=="Ask":
                            append_order_dataframe.at[0,"MY_OPERATIONAL_UTILITY_FIAT"]=append_order_dataframe.at[0,"TOTAL_EXCHANGED"]
                        else:
                            append_order_dataframe.at[0,"MY_OPERATIONAL_UTILITY_FIAT"]=-append_order_dataframe.at[0,"TOTAL_EXCHANGED"]
                        append_order_dataframe.at[0,"MY_OPERATIONAL_UTILITY_USD"]=append_order_dataframe.at[0,"MY_OPERATIONAL_UTILITY_FIAT"]/append_order_dataframe.at[0,"MY_TRM"]
                        
                        #_____DAR FORMATO A COLUMNAS NUEVAS
                        append_order_dataframe.MY_CRYPTO=append_order_dataframe.MY_CRYPTO.astype(float)
                        append_order_dataframe.MY_FIAT=append_order_dataframe.MY_FIAT.astype(float)
                        append_order_dataframe.MY_TRM=append_order_dataframe.MY_TRM.astype(float)
                        append_order_dataframe.MY_CRYPTO_IN_FIAT=append_order_dataframe.MY_CRYPTO_IN_FIAT.astype(float)
                        append_order_dataframe.MY_CRYPTO_IN_USD=append_order_dataframe.MY_CRYPTO_IN_USD.astype(float)
                        append_order_dataframe.MY_FIAT_IN_USD=append_order_dataframe.MY_FIAT_IN_USD.astype(float)
                        append_order_dataframe.MY_MARKET_USD=append_order_dataframe.MY_MARKET_USD.astype(float)
                        append_order_dataframe.ORIGINAL_AMOUNT_USD=append_order_dataframe.ORIGINAL_AMOUNT_USD.astype(float)
                        append_order_dataframe.MY_EXECUTED_AMOUNT_USD=append_order_dataframe.MY_EXECUTED_AMOUNT_USD.astype(float)
                        append_order_dataframe.MY_OPERATIONAL_UTILITY_FIAT=append_order_dataframe.MY_OPERATIONAL_UTILITY_FIAT.astype(float)
                        append_order_dataframe.MY_OPERATIONAL_UTILITY_USD=append_order_dataframe.MY_OPERATIONAL_UTILITY_USD.astype(float)

                        #_____SUBIR FILAS FALTANTES A GOOGLE CLOUD
                        bigquery_client.insert_rows(bigquery_client.get_table(table_ref), append_order_dataframe.values.tolist())

            #_____SI TENGO BIDS PENDIENTES
            if len(bidIdList)>0:

                for i in bidIdList:

                    time.sleep(sleepApis)
                    
                    #_____ACTUALIZAR DETALLES DE ÓRDENES
                    while True:
                        try:
                            bidOrderDetailsFinish = client.order_details(i)
                            break
                        except:
                            print("[ERROR]: finishThemAll() -> bidOrderDetailsFinish = client.order_details(i)")
                            time.sleep(sleepError)

                    #_____SI EJECUTÉ PARCIAL O TOTALEMTNE LA ÓRDEN -> GUARDAR PRECIOS
                    if (bidOrderDetailsFinish.traded_amount.amount > 0.0):
                        theoryBuyExecuted=theoryBuyPrice
                        write_buy_sell_prices()
                    
                    time.sleep(sleepApis)

                    #_____ITERAR HASTA QUE SE CANCELE LA ORDEN
                    while True:
                        try:
                            client.cancel_order(i)
                            break
                        except:
                            print("[ERROR]: finishThemAll() -> client.cancel_order(i)")
                            time.sleep(sleepError)

                    #_____ITERAR HASTA QUE SE ACTUALICE EL ESTADO DE LA ORDEN
                    while(True):

                        time.sleep(sleepApis)

                        while True:
                            try:
                                bidOrderDetailsFinish = client.order_details(i)
                                break
                            except:
                                print("[ERROR]: finishThemAll() -> bidOrderDetailsFinish = client.order_details(i)(1)")
                                time.sleep(sleepError)

                        #_____SI LA ORDEN YA SE ENCUENTRA CANCELADA -> SALIR DEL LOOP
                        if (bidOrderDetailsFinish.state=="canceled") or (bidOrderDetailsFinish.state=="traded"):
                            break
                        else:
                            while True:
                                time.sleep(sleepApis)
                                try:
                                    client.cancel_order(i)
                                    break
                                except:
                                    print("[ERROR]: finishThemAll() -> bidOrderDetailsFinish = client.cancel_order(i)(2)")
                                    time.sleep(sleepError)

                    #_____SI EJECUTÉ PARCIAL O TOTALEMTNE LA ÓRDEN -> GUARDAR PRECIOS
                    if (bidOrderDetailsFinish.traded_amount.amount > 0.0):

                        #_____CREAR CONEXIÓN CON BASE DE DATOS EN BIGQUERY
                        os.environ["GOOGLE_APPLICATION_CREDENTIALS"]='gcp_json.json'
                        bigquery_client=bigquery.Client(project="dogwood-terra-308100")
                        dataset_ref=bigquery_client.dataset("spreadNet")
                        table_ref=dataset_ref.table(CRYPT+"_"+MONEY)
                        table=dataset_ref.table(CRYPT+"_"+MONEY)
                        table=bigquery.Table(table)

                        #_____COLUMNAS
                        columns=['ID','UUID','MARKET_ID','ACCOUNT_ID','TYPE','STATE','CREATED_AT','FEE_CURRENCY','PRICE_TYPE','SOURCE','LIMIT','AMOUNT','ORIGINAL_AMOUNT','TRADED_AMOUNT','TOTAL_EXCHANGED','PAID_FEE']

                        #_____CREAR BASE DE DATOS PARA REGISTRO DE ORDEN
                        append_order_dataframe=pd.DataFrame(bidOrderDetailsFinish.json).head(1)
                        append_order_dataframe.columns=columns

                        #_____QUITAR COLUMNAS INNECESARIAS
                        append_order_dataframe=append_order_dataframe[["ID","ACCOUNT_ID","AMOUNT","CREATED_AT","FEE_CURRENCY","LIMIT","MARKET_ID","ORIGINAL_AMOUNT","PAID_FEE","PRICE_TYPE","STATE","TOTAL_EXCHANGED","TRADED_AMOUNT","TYPE"]]

                        #_____DAR FORMATO A COLUMNAS
                        append_order_dataframe.ID=append_order_dataframe.ID.astype(str)
                        append_order_dataframe.ACCOUNT_ID=append_order_dataframe.ACCOUNT_ID.astype(str)
                        append_order_dataframe.AMOUNT=append_order_dataframe.AMOUNT.astype(float)
                        append_order_dataframe["CREATED_AT"]=pd.to_datetime(append_order_dataframe["CREATED_AT"])
                        append_order_dataframe.FEE_CURRENCY=append_order_dataframe.FEE_CURRENCY.astype(str)
                        append_order_dataframe.LIMIT=append_order_dataframe.LIMIT.astype(float)
                        append_order_dataframe.MARKET_ID=append_order_dataframe.MARKET_ID.astype(str)
                        append_order_dataframe.ORIGINAL_AMOUNT=append_order_dataframe.ORIGINAL_AMOUNT.astype(float)
                        append_order_dataframe.PAID_FEE=append_order_dataframe.PAID_FEE.astype(float)
                        append_order_dataframe.PRICE_TYPE=append_order_dataframe.PRICE_TYPE.astype(str)
                        append_order_dataframe.STATE=append_order_dataframe.STATE.astype(str)
                        append_order_dataframe.TOTAL_EXCHANGED=append_order_dataframe.TOTAL_EXCHANGED.astype(float)
                        append_order_dataframe.TRADED_AMOUNT=append_order_dataframe.TRADED_AMOUNT.astype(float)
                        append_order_dataframe.TYPE=append_order_dataframe.TYPE.astype(str)
                        
                        #_____AGREGAR COLUMNAS FALTANTES
                        append_order_dataframe.at[0,"MY_CRYPTO"]=getCRYinAccount()
                        append_order_dataframe.at[0,"MY_FIAT"]=getMONinAccount()
                        append_order_dataframe.at[0,"MY_TRM"]=getFiatUsdQuote(MONEY)
                        append_order_dataframe.at[0,"MY_CRYPTO_IN_FIAT"]=append_order_dataframe.at[0,"MY_CRYPTO"]*append_order_dataframe.at[0,"LIMIT"]
                        append_order_dataframe.at[0,"MY_CRYPTO_IN_USD"]=append_order_dataframe.at[0,"MY_CRYPTO_IN_FIAT"]/append_order_dataframe.at[0,"MY_TRM"]
                        append_order_dataframe.at[0,"MY_FIAT_IN_USD"]=append_order_dataframe.at[0,"MY_FIAT"]/append_order_dataframe.at[0,"MY_TRM"]
                        append_order_dataframe.at[0,"MY_MARKET_USD"]=append_order_dataframe.at[0,"MY_CRYPTO_IN_USD"]+append_order_dataframe.at[0,"MY_FIAT_IN_USD"]
                        append_order_dataframe.at[0,"ORIGINAL_AMOUNT_USD"]=(append_order_dataframe.at[0,"ORIGINAL_AMOUNT"]*append_order_dataframe.at[0,"LIMIT"])/append_order_dataframe.at[0,"MY_TRM"]
                        append_order_dataframe.at[0,"MY_EXECUTED_AMOUNT_USD"]=append_order_dataframe.at[0,"TOTAL_EXCHANGED"]/append_order_dataframe.at[0,"MY_TRM"]
                        if append_order_dataframe.at[0,"TYPE"]=="Ask":
                            append_order_dataframe.at[0,"MY_OPERATIONAL_UTILITY_FIAT"]=append_order_dataframe.at[0,"TOTAL_EXCHANGED"]
                        else:
                            append_order_dataframe.at[0,"MY_OPERATIONAL_UTILITY_FIAT"]=-append_order_dataframe.at[0,"TOTAL_EXCHANGED"]
                        append_order_dataframe.at[0,"MY_OPERATIONAL_UTILITY_USD"]=append_order_dataframe.at[0,"MY_OPERATIONAL_UTILITY_FIAT"]/append_order_dataframe.at[0,"MY_TRM"]

                        #_____DAR FORMATO A NUEVAS COLUMNAS
                        append_order_dataframe.MY_CRYPTO=append_order_dataframe.MY_CRYPTO.astype(float)
                        append_order_dataframe.MY_FIAT=append_order_dataframe.MY_FIAT.astype(float)
                        append_order_dataframe.MY_TRM=append_order_dataframe.MY_TRM.astype(float)
                        append_order_dataframe.MY_CRYPTO_IN_FIAT=append_order_dataframe.MY_CRYPTO_IN_FIAT.astype(float)
                        append_order_dataframe.MY_CRYPTO_IN_USD=append_order_dataframe.MY_CRYPTO_IN_USD.astype(float)
                        append_order_dataframe.MY_FIAT_IN_USD=append_order_dataframe.MY_FIAT_IN_USD.astype(float)
                        append_order_dataframe.MY_MARKET_USD=append_order_dataframe.MY_MARKET_USD.astype(float)
                        append_order_dataframe.ORIGINAL_AMOUNT_USD=append_order_dataframe.ORIGINAL_AMOUNT_USD.astype(float)
                        append_order_dataframe.MY_EXECUTED_AMOUNT_USD=append_order_dataframe.MY_EXECUTED_AMOUNT_USD.astype(float)
                        append_order_dataframe.MY_OPERATIONAL_UTILITY_FIAT=append_order_dataframe.MY_OPERATIONAL_UTILITY_FIAT.astype(float)
                        append_order_dataframe.MY_OPERATIONAL_UTILITY_USD=append_order_dataframe.MY_OPERATIONAL_UTILITY_USD.astype(float)

                        #_____SUBIR FILAS FALTANTES A GOOGLE CLOUD
                        bigquery_client.insert_rows(bigquery_client.get_table(table_ref), append_order_dataframe.values.tolist())

        balancing_Ask_Bid()

# TERMINA ORDENES PARA TESTEO
def finishThemAllTest():

    global MONEY
    global CRYPT
    global client
    global API_KEY
    global sleepApis
    global API_SECRET
    global bidOrderId
    global askOrderId
    global sleepError
    global client_surbtc
    global theoryBuyPrice
    global bidOrderDetails
    global askOrderDetails
    global theorySellPrice
    global theoryBuyExecuted
    global theorySellExecuted

    #_____PARAMETERS
    while True:
        try:
            Market = client_surbtc.getMarket(CRYPT+"-"+MONEY)
            data=pd.DataFrame(Market.getPendingOrders())
            break
        except:
            print("[ERROR]: finishThemAll() -> client_surbtc")
            time.sleep(sleepError)
    
    #_____SI TENGO ÓRDENES
    if len(data)>0:
    
        askPendings=len(data.loc[((data.state=="pending") | (data.state=="accepted")) & (data.type=="Ask")])
        bidPendings=len(data.loc[((data.state=="pending") | (data.state=="accepted")) & (data.type=="Bid")])

        #_____SI TENGO ÓRDENES EJECUTADAS PARCIAL O TOTALMENTE
        if (askPendings>0) or (bidPendings>0):

            askIdList=list(data.loc[((data.state=="pending") | (data.state=="accepted")) & (data.type=="Ask")].id.values)
            bidIdList=list(data.loc[((data.state=="pending") | (data.state=="accepted")) & (data.type=="Bid")].id.values)

            #_____SI TENGO ASKS PENDIENTES
            if len(askIdList)>0:

                for i in askIdList:

                    time.sleep(sleepApis)
                    
                    #_____ACTUALIZAR DETALLES DE ÓRDENES
                    while True:
                        try:
                            askOrderDetailsFinish = client.order_details(i)
                            break
                        except:
                            print("[ERROR]: finishThemAll() -> askOrderDetailsFinish = client.order_details(i)")
                            time.sleep(sleepError)

                    #_____SI EJECUTÉ PARCIAL O TOTALEMTNE LA ÓRDEN -> GUARDAR PRECIOS
                    if (askOrderDetailsFinish.traded_amount.amount > 0.0):
                        theorySellExecuted=theorySellPrice
                        write_buy_sell_prices()
                    
                    time.sleep(sleepApis)

                    #_____ITERAR HASTA QUE SE CANCELE LA ORDEN
                    while True:
                        try:
                            client.cancel_order(i)
                            break
                        except:
                            print("[ERROR]: finishThemAll() -> client.cancel_order(i)")
                            time.sleep(sleepError)

                    #_____ITERAR HASTA QUE SE ACTUALICE EL ESTADO DE LA ORDEN
                    while(True):

                        time.sleep(sleepApis)

                        while True:
                            try:
                                askOrderDetailsFinish = client.order_details(i)
                                break
                            except:
                                print("[ERROR]: finishThemAll() -> askOrderDetailsFinish = client.order_details(i)(1)")
                                time.sleep(sleepError)

                        #_____SI LA ORDEN YA SE ENCUENTRA CANCELADA -> SALIR DEL LOOP
                        if (askOrderDetailsFinish.state=="canceled") or (askOrderDetailsFinish.state=="traded"):
                            break
                        else:
                            while True:
                                time.sleep(sleepApis)
                                try:
                                    client.cancel_order(i)
                                    break
                                except:
                                    print("[ERROR]: finishThemAll() -> askOrderDetailsFinish = client.cancel_order(i)(2)")
                                    time.sleep(sleepError)

            #_____SI TENGO BIDS PENDIENTES
            if len(bidIdList)>0:

                for i in bidIdList:

                    time.sleep(sleepApis)
                    
                    #_____ACTUALIZAR DETALLES DE ÓRDENES
                    while True:
                        try:
                            bidOrderDetailsFinish = client.order_details(i)
                            break
                        except:
                            print("[ERROR]: finishThemAll() -> bidOrderDetailsFinish = client.order_details(i)")
                            time.sleep(sleepError)

                    #_____SI EJECUTÉ PARCIAL O TOTALEMTNE LA ÓRDEN -> GUARDAR PRECIOS
                    if (bidOrderDetailsFinish.traded_amount.amount > 0.0):
                        theoryBuyExecuted=theoryBuyPrice
                        write_buy_sell_prices()
                    
                    time.sleep(sleepApis)

                    #_____ITERAR HASTA QUE SE CANCELE LA ORDEN
                    while True:
                        try:
                            client.cancel_order(i)
                            break
                        except:
                            print("[ERROR]: finishThemAll() -> client.cancel_order(i)")
                            time.sleep(sleepError)

                    #_____ITERAR HASTA QUE SE ACTUALICE EL ESTADO DE LA ORDEN
                    while(True):

                        time.sleep(sleepApis)

                        while True:
                            try:
                                bidOrderDetailsFinish = client.order_details(i)
                                break
                            except:
                                print("[ERROR]: finishThemAll() -> bidOrderDetailsFinish = client.order_details(i)(1)")
                                time.sleep(sleepError)

                        #_____SI LA ORDEN YA SE ENCUENTRA CANCELADA -> SALIR DEL LOOP
                        if (bidOrderDetailsFinish.state=="canceled") or (bidOrderDetailsFinish.state=="traded"):
                            break
                        else:
                            while True:
                                time.sleep(sleepApis)
                                try:
                                    client.cancel_order(i)
                                    break
                                except:
                                    print("[ERROR]: finishThemAll() -> bidOrderDetailsFinish = client.cancel_order(i)(2)")
                                    time.sleep(sleepError)

# DESCARGAR ORDERBOOK DE BUDA
def request_order_book():
    
    global URL
    global API_KEY
    global sleepApis
    global sleepError
    global API_SECRET
    
    while True:
        try:
            time.sleep(sleepApis)
            with requests.get(URL, auth=BudaHMACAuth(API_KEY, API_SECRET)) as r:
                order_book = r.json()
                if ( order_book != None and 'order_book' in order_book ):
                    return order_book['order_book']
                    break
                else:
                    print('[[ERROR]]: request_order_book()')
                    time.sleep(sleepError)
        except:
            print('[[ERROR]]: request_order_book()')
            time.sleep(sleepError)

# ACTUALIZAR PRECIOS LÍMITES DE LIBRO DE ÓRDENES + ACTUALIZAR TOPES
def updateLimits():
    
    global askVolume
    global bidVolume
    global priceDistance
    global limitAskPrice
    global limitBidPrice
    global limitAskVolume
    global limitBidVolume
    global tradeProportion
    
    order_book = request_order_book()
    
    limitAskPrice = float(order_book['asks'][0][0])
    limitBidPrice = float(order_book['bids'][0][0])
    limitAskVolume = float(order_book['asks'][0][1])
    limitBidVolume = float(order_book['bids'][0][1])

    if (float(askVolume/(limitAskVolume+askVolume))<tradeProportion) and (askVolume>0.0):
        limitAskPrice -= priceDistance
    if (float(bidVolume/(limitBidVolume+bidVolume))<tradeProportion) and (bidVolume>0.0):
        limitBidPrice += priceDistance

# ACTUALIZAR PRECIOS
def updatePriceVolume():
    
    global newAskPrice
    global newBidPrice
    global newLimitAskVolume        
    global newLimitBidVolume
    global newPreLimitAskPrice
    global newPreLimitBidPrice

    order_book = request_order_book()

    newAskPrice = float(order_book['asks'][0][0])
    newBidPrice = float(order_book['bids'][0][0])
    newLimitAskVolume = float(order_book['asks'][0][1])
    newLimitBidVolume = float(order_book['bids'][0][1])
    newPreLimitAskPrice = float(request_order_book()["asks"][1][0])
    newPreLimitBidPrice = float(request_order_book()["bids"][1][0])

# VALIDAR MARGEN DE UTILIDAD
def validMargin(limitAskPrice, limitBidPrice):
    return (limitAskPrice/limitBidPrice)-1

# CANCEL ASK ORDER + WRITE BIGQUERY DATABSE
def cancelAsk():

    global CRYPT
    global MONEY
    global client
    global sleepApis
    global sleepError
    global askOrderId
    global gotAskOrder
    global client_surbtc
    global theorySellPrice
    global askOrderDetails
    global theorySellExecuted

    time.sleep(sleepApis)

    #_____SI TENGO UNA ORDEN MONTADA (TENGO EL ID)
    if askOrderId != None:

        #_____ACTUALIZAR DETALLES DE LA ORDEN
        while True:
            try:
                askOrderDetails = client.order_details(askOrderId)
                break
            except:
                print("[[ERROR]]: cancelAsk() -> askOrderDetails = client.order_details(askOrderId)")
                time.sleep(sleepError)
        
        #_____SI LA ORDEN SE EJECUTÓ PARCIAL O TOTALMENTE
        if (askOrderDetails.traded_amount.amount > 0.0):
            theorySellExecuted=theorySellPrice
            write_buy_sell_prices()
        
        time.sleep(sleepApis)

        #_____CANCELAR LA ORDEN A COMO DE LUGAR
        while True:
            try:
                client.cancel_order(askOrderId)
                break
            except:
                print("[[ERROR]]: cancelAsk() -> askOrderDetails = client.cancel_order(askOrderId)")
                time.sleep(sleepError)

        #_____ACTUALIZAR DETALLES DE LA ORDEN
        while True:

            time.sleep(sleepApis)

            while True:
                try:
                    askOrderDetails = client.order_details(askOrderId)
                    break
                except:
                    print("[[ERROR]]: cancelAsk() -> askOrderDetails = client.order_details(askOrderId) (2)")
                    time.sleep(sleepError)

            #_____SI LA ORDEN YA SE MUESTRA COMO CANCELADA O TRANSADA
            if (askOrderDetails.state=="canceled") or (askOrderDetails.state=="traded"):
                break
            else:
                
                time.sleep(sleepApis)

                #_____CANCELAR LA ORDEN
                while True:
                    try:
                        client.cancel_order(askOrderId)
                        break
                    except:
                        print("[[ERROR]]: cancelAsk() -> askOrderDetails = client.cancel_order(askOrderId) (2)")
        
        #_____SI LA ORDEN SE EJECUTÓ PARCIAL O TOTALMENTE
        if (askOrderDetails.traded_amount.amount > 0.0):

            #_____CREAR CONEXIÓN CON BASE DE DATOS EN BIGQUERY
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"]='gcp_json.json'
            bigquery_client=bigquery.Client(project="dogwood-terra-308100")
            dataset_ref=bigquery_client.dataset("spreadNet")
            table_ref=dataset_ref.table(CRYPT+"_"+MONEY)
            table=dataset_ref.table(CRYPT+"_"+MONEY)
            table=bigquery.Table(table)

            #_____ASIGNAR COLUMNAS A BASE DE DATOS DE LA ORDEN CANCELADA
            columns=["ID","UUID","MARKET_ID","ACCOUNT_ID","TYPE","STATE","CREATED_AT","FEE_CURRENCY","PRICE_TYPE","SOURCE","LIMIT","AMOUNT","ORIGINAL_AMOUNT","TRADED_AMOUNT","TOTAL_EXCHANGED","PAID_FEE"]
        
            #_____CREAR BASE DE DATOS PARA REGISTRO DE ORDEN
            append_order_dataframe=pd.DataFrame(askOrderDetails.json).head(1)
            append_order_dataframe.columns=columns

            #_____QUITAR COLUMNAS INNECESARIAS
            append_order_dataframe=append_order_dataframe[["ID","ACCOUNT_ID","AMOUNT","CREATED_AT","FEE_CURRENCY","LIMIT","MARKET_ID","ORIGINAL_AMOUNT","PAID_FEE","PRICE_TYPE","STATE","TOTAL_EXCHANGED","TRADED_AMOUNT","TYPE"]]

            #_____DAR FORMATO A COLUMNAS
            append_order_dataframe.ID=append_order_dataframe.ID.astype(str)
            append_order_dataframe.ACCOUNT_ID=append_order_dataframe.ACCOUNT_ID.astype(str)
            append_order_dataframe.AMOUNT=append_order_dataframe.AMOUNT.astype(float)
            append_order_dataframe.CREATED_AT= pd.to_datetime(append_order_dataframe.CREATED_AT)
            append_order_dataframe.FEE_CURRENCY=append_order_dataframe.FEE_CURRENCY.astype(str)
            append_order_dataframe.LIMIT=append_order_dataframe.LIMIT.astype(float)
            append_order_dataframe.MARKET_ID=append_order_dataframe.MARKET_ID.astype(str)
            append_order_dataframe.ORIGINAL_AMOUNT=append_order_dataframe.ORIGINAL_AMOUNT.astype(float)
            append_order_dataframe.PAID_FEE=append_order_dataframe.PAID_FEE.astype(float)
            append_order_dataframe.PRICE_TYPE=append_order_dataframe.PRICE_TYPE.astype(str)
            append_order_dataframe.STATE=append_order_dataframe.STATE.astype(str)
            append_order_dataframe.TOTAL_EXCHANGED=append_order_dataframe.TOTAL_EXCHANGED.astype(float)
            append_order_dataframe.TRADED_AMOUNT=append_order_dataframe.TRADED_AMOUNT.astype(float)
            append_order_dataframe.TYPE=append_order_dataframe.TYPE.astype(str)

            #_____AGREGAR COLUMNAS FALTANTES
            append_order_dataframe.at[0,"MY_CRYPTO"]=getCRYinAccount()
            append_order_dataframe.at[0,"MY_FIAT"]=getMONinAccount()
            append_order_dataframe.at[0,"MY_TRM"]=getFiatUsdQuote(MONEY)
            append_order_dataframe.at[0,"MY_CRYPTO_IN_FIAT"]=append_order_dataframe.at[0,"MY_CRYPTO"]*append_order_dataframe.at[0,"LIMIT"]
            append_order_dataframe.at[0,"MY_CRYPTO_IN_USD"]=append_order_dataframe.at[0,"MY_CRYPTO_IN_FIAT"]/append_order_dataframe.at[0,"MY_TRM"]
            append_order_dataframe.at[0,"MY_FIAT_IN_USD"]=append_order_dataframe.at[0,"MY_FIAT"]/append_order_dataframe.at[0,"MY_TRM"]
            append_order_dataframe.at[0,"MY_MARKET_USD"]=append_order_dataframe.at[0,"MY_CRYPTO_IN_USD"]+append_order_dataframe.at[0,"MY_FIAT_IN_USD"]
            append_order_dataframe.at[0,"ORIGINAL_AMOUNT_USD"]=append_order_dataframe.at[0,"ORIGINAL_AMOUNT"]*append_order_dataframe.at[0,"LIMIT"]/append_order_dataframe.at[0,"MY_TRM"]
            append_order_dataframe.at[0,"MY_EXECUTED_AMOUNT_USD"]=append_order_dataframe.at[0,"TOTAL_EXCHANGED"]/append_order_dataframe.at[0,"MY_TRM"]
            if append_order_dataframe.at[0,"TYPE"]=="Ask":
                append_order_dataframe.at[0,"MY_OPERATIONAL_UTILITY_FIAT"]=append_order_dataframe.at[0,"TOTAL_EXCHANGED"]
            else:
                append_order_dataframe.at[0,"MY_OPERATIONAL_UTILITY_FIAT"]=-append_order_dataframe.at[0,"TOTAL_EXCHANGED"]
            append_order_dataframe.at[0,"MY_OPERATIONAL_UTILITY_USD"]=append_order_dataframe.at[0,"MY_OPERATIONAL_UTILITY_FIAT"]/append_order_dataframe.at[0,"MY_TRM"]
            
            #_____DAR FORMATO A NUEVAS COLUMNAS
            append_order_dataframe.MY_CRYPTO=append_order_dataframe.MY_CRYPTO.astype(float)
            append_order_dataframe.MY_FIAT=append_order_dataframe.MY_FIAT.astype(float)
            append_order_dataframe.MY_TRM=append_order_dataframe.MY_TRM.astype(float)
            append_order_dataframe.MY_CRYPTO_IN_FIAT=append_order_dataframe.MY_CRYPTO_IN_FIAT.astype(float)
            append_order_dataframe.MY_CRYPTO_IN_USD=append_order_dataframe.MY_CRYPTO_IN_USD.astype(float)
            append_order_dataframe.MY_FIAT_IN_USD=append_order_dataframe.MY_FIAT_IN_USD.astype(float)
            append_order_dataframe.MY_MARKET_USD=append_order_dataframe.MY_MARKET_USD.astype(float)
            append_order_dataframe.ORIGINAL_AMOUNT_USD=append_order_dataframe.ORIGINAL_AMOUNT_USD.astype(float)
            append_order_dataframe.MY_EXECUTED_AMOUNT_USD=append_order_dataframe.MY_EXECUTED_AMOUNT_USD.astype(float)
            append_order_dataframe.MY_OPERATIONAL_UTILITY_FIAT=append_order_dataframe.MY_OPERATIONAL_UTILITY_FIAT.astype(float)
            append_order_dataframe.MY_OPERATIONAL_UTILITY_USD=append_order_dataframe.MY_OPERATIONAL_UTILITY_USD.astype(float)

            #_____SUBIR FILAS FALTANTES A GOOGLE CLOUD
            bigquery_client.insert_rows(bigquery_client.get_table(table_ref), append_order_dataframe.values.tolist())

        #_____ACTUALIZAR VARIABLES
        askOrderDetails=None
        askOrderId=None

    #_____SI NO TENGO ID DE LA ORDEN, PERO QUIERO TRATAR DE CANCELAR
    else:

        #_____PARAMETERS
        askPendings=0
        askOrderId=None
        askOrderDetails=None

        time.sleep(sleepApis)
        
        while True:
            try:
                Market = client_surbtc.getMarket(CRYPT+"-"+MONEY)
                data=pd.DataFrame(Market.getPendingOrders())
                break
            except:
                print("[ERROR]: finishThemAll() -> client_surbtc")
                time.sleep(sleepError)
 
        #_____SI LEN DATA ES MAYOR A CERO
        if len(data)>0:

            askPendings=len(data.loc[((data.state=="pending") | (data.state=="accepted")) & (data.type=="Ask")])

            #_____SI TENGO ÓRDENES EJECUTADAS PARCIAL O TOTALMENTE
            if (askPendings>0):
                
                askIdList=list(data.loc[((data.state=="pending") | (data.state=="accepted")) & (data.type=="Ask")].id.values)
            
                #_____SI TENGO ASKS PENDIENTES
                if len(askIdList)>0:

                    for i in askIdList:

                        time.sleep(sleepApis)
                        
                        #_____ACTUALIZAR DETALLES DE ÓRDENES
                        while True:
                            try:
                                askOrderDetailsFinish = client.order_details(i)
                                break
                            except:
                                print("[ERROR]: finishThemAll() -> askOrderDetailsFinish = client.order_details(i)")
                                time.sleep(sleepError)

                        #_____SI EJECUTÉ PARCIAL O TOTALEMTNE LA ÓRDEN -> GUARDAR PRECIOS
                        if (askOrderDetailsFinish.traded_amount.amount > 0.0):
                            theorySellExecuted=theorySellPrice
                            write_buy_sell_prices()
                        
                        #_____ITERAR HASTA QUE SE CANCELE LA ORDEN
                        while True:
                            time.sleep(sleepApis)
                            try:
                                client.cancel_order(i)
                                break
                            except:
                                print("[ERROR]: finishThemAll() -> client.cancel_order(i)")
                                time.sleep(sleepError)

                        #_____ITERAR HASTA QUE SE ACTUALICE EL ESTADO DE LA ORDEN
                        while(True):

                            time.sleep(sleepApis)

                            while True:
                                try:
                                    askOrderDetailsFinish = client.order_details(i)
                                    break
                                except:
                                    print("[ERROR]: finishThemAll() -> askOrderDetailsFinish = client.order_details(i)(1)")
                                    time.sleep(sleepError)

                            #_____SI LA ORDEN YA SE ENCUENTRA CANCELADA -> SALIR DEL LOOP
                            if (askOrderDetailsFinish.state=="canceled") or (askOrderDetailsFinish.state=="traded"):
                                break
                            else:
                                while True:
                                    time.sleep(sleepApis)
                                    try:
                                        client.cancel_order(i)
                                        break
                                    except:
                                        print("[ERROR]: finishThemAll() -> askOrderDetailsFinish = client.cancel_order(i)(2)")
                                        time.sleep(sleepError)

                        #_____SI EJECUTÉ PARCIAL O TOTALEMTNE LA ÓRDEN -> GUARDAR PRECIOS
                        if (askOrderDetailsFinish.traded_amount.amount > 0.0):

                            #_____CREAR CONEXIÓN CON BASE DE DATOS EN BIGQUERY
                            os.environ["GOOGLE_APPLICATION_CREDENTIALS"]='gcp_json.json'
                            bigquery_client=bigquery.Client(project="dogwood-terra-308100")
                            dataset_ref=bigquery_client.dataset("spreadNet")
                            table_ref=dataset_ref.table(CRYPT+"_"+MONEY)
                            table=dataset_ref.table(CRYPT+"_"+MONEY)
                            table=bigquery.Table(table)

                            #_____COLUMNAS
                            columns=["ID","UUID","MARKET_ID","ACCOUNT_ID","TYPE","STATE","CREATED_AT","FEE_CURRENCY","PRICE_TYPE","SOURCE","LIMIT","AMOUNT","ORIGINAL_AMOUNT","TRADED_AMOUNT","TOTAL_EXCHANGED","PAID_FEE"]
        
                            #_____CREAR BASE DE DATOS PARA REGISTRO DE ORDEN
                            append_order_dataframe=pd.DataFrame(askOrderDetails.json).head(1)
                            append_order_dataframe.columns=columns

                            #_____QUITAR COLUMNAS INNECESARIAS
                            append_order_dataframe=append_order_dataframe[["ID","ACCOUNT_ID","AMOUNT","CREATED_AT","FEE_CURRENCY","LIMIT","MARKET_ID","ORIGINAL_AMOUNT","PAID_FEE","PRICE_TYPE","STATE","TOTAL_EXCHANGED","TRADED_AMOUNT","TYPE"]]

                            #_____DAR FORMATO A COLUMNAS
                            append_order_dataframe.ID=append_order_dataframe.ID.astype(str)
                            append_order_dataframe.ACCOUNT_ID=append_order_dataframe.ACCOUNT_ID.astype(str)
                            append_order_dataframe.AMOUNT=append_order_dataframe.AMOUNT.astype(float)
                            append_order_dataframe["CREATED_AT"]=pd.to_datetime(append_order_dataframe["CREATED_AT"])
                            append_order_dataframe.FEE_CURRENCY=append_order_dataframe.FEE_CURRENCY.astype(str)
                            append_order_dataframe.LIMIT=append_order_dataframe.LIMIT.astype(float)
                            append_order_dataframe.MARKET_ID=append_order_dataframe.MARKET_ID.astype(str)
                            append_order_dataframe.ORIGINAL_AMOUNT=append_order_dataframe.ORIGINAL_AMOUNT.astype(float)
                            append_order_dataframe.PAID_FEE=append_order_dataframe.PAID_FEE.astype(float)
                            append_order_dataframe.PRICE_TYPE=append_order_dataframe.PRICE_TYPE.astype(str)
                            append_order_dataframe.STATE=append_order_dataframe.STATE.astype(str)
                            append_order_dataframe.TOTAL_EXCHANGED=append_order_dataframe.TOTAL_EXCHANGED.astype(float)
                            append_order_dataframe.TRADED_AMOUNT=append_order_dataframe.TRADED_AMOUNT.astype(float)
                            append_order_dataframe.TYPE=append_order_dataframe.TYPE.astype(str)

                            #_____AGREGAR COLUMNAS FALTANTES
                            append_order_dataframe.at[0,"MY_CRYPTO"]=getCRYinAccount()
                            append_order_dataframe.at[0,"MY_FIAT"]=getMONinAccount()
                            append_order_dataframe.at[0,"MY_TRM"]=getFiatUsdQuote(MONEY)
                            append_order_dataframe.at[0,"MY_CRYPTO_IN_FIAT"]=append_order_dataframe.at[0,"MY_CRYPTO"]*append_order_dataframe.at[0,"LIMIT"]
                            append_order_dataframe.at[0,"MY_CRYPTO_IN_USD"]=append_order_dataframe.at[0,"MY_CRYPTO_IN_FIAT"]/append_order_dataframe.at[0,"MY_TRM"]
                            append_order_dataframe.at[0,"MY_FIAT_IN_USD"]=append_order_dataframe.at[0,"MY_FIAT"]/append_order_dataframe.at[0,"MY_TRM"]
                            append_order_dataframe.at[0,"MY_MARKET_USD"]=append_order_dataframe.at[0,"MY_CRYPTO_IN_USD"]+append_order_dataframe.at[0,"MY_FIAT_IN_USD"]
                            append_order_dataframe.at[0,"ORIGINAL_AMOUNT_USD"]=(append_order_dataframe.at[0,"ORIGINAL_AMOUNT"]*append_order_dataframe.at[0,"LIMIT"])/append_order_dataframe.at[0,"MY_TRM"]
                            append_order_dataframe.at[0,"MY_EXECUTED_AMOUNT_USD"]=append_order_dataframe.at[0,"TOTAL_EXCHANGED"]/append_order_dataframe.at[0,"MY_TRM"]
                            if append_order_dataframe.at[0,"TYPE"]=="Ask":
                                append_order_dataframe.at[0,"MY_OPERATIONAL_UTILITY_FIAT"]=append_order_dataframe.at[0,"TOTAL_EXCHANGED"]
                            else:
                                append_order_dataframe.at[0,"MY_OPERATIONAL_UTILITY_FIAT"]=-append_order_dataframe.at[0,"TOTAL_EXCHANGED"]
                            append_order_dataframe.at[0,"MY_OPERATIONAL_UTILITY_USD"]=append_order_dataframe.at[0,"MY_OPERATIONAL_UTILITY_FIAT"]/append_order_dataframe.at[0,"MY_TRM"]

                            #_____DAR FORMATO A COLUMNAS NUEVAS
                            append_order_dataframe.MY_CRYPTO=append_order_dataframe.MY_CRYPTO.astype(float)
                            append_order_dataframe.MY_FIAT=append_order_dataframe.MY_FIAT.astype(float)
                            append_order_dataframe.MY_TRM=append_order_dataframe.MY_TRM.astype(float)
                            append_order_dataframe.MY_CRYPTO_IN_FIAT=append_order_dataframe.MY_CRYPTO_IN_FIAT.astype(float)
                            append_order_dataframe.MY_CRYPTO_IN_USD=append_order_dataframe.MY_CRYPTO_IN_USD.astype(float)
                            append_order_dataframe.MY_FIAT_IN_USD=append_order_dataframe.MY_FIAT_IN_USD.astype(float)
                            append_order_dataframe.MY_MARKET_USD=append_order_dataframe.MY_MARKET_USD.astype(float)
                            append_order_dataframe.ORIGINAL_AMOUNT_USD=append_order_dataframe.ORIGINAL_AMOUNT_USD.astype(float)
                            append_order_dataframe.MY_EXECUTED_AMOUNT_USD=append_order_dataframe.MY_EXECUTED_AMOUNT_USD.astype(float)
                            append_order_dataframe.MY_OPERATIONAL_UTILITY_FIAT=append_order_dataframe.MY_OPERATIONAL_UTILITY_FIAT.astype(float)
                            append_order_dataframe.MY_OPERATIONAL_UTILITY_USD=append_order_dataframe.MY_OPERATIONAL_UTILITY_USD.astype(float)

                            #_____SUBIR FILAS FALTANTES A GOOGLE CLOUD
                            bigquery_client.insert_rows(bigquery_client.get_table(table_ref), append_order_dataframe.values.tolist())

    #_____PRINT
    print("CANCELO ASK")
        
# CREAR ORDEN ASK (LIMIT)
def createAsk(limitAskPrice):
    
    global MONEY
    global CRYPT
    global client
    global pastAsks
    global pastBids
    global sleepApis
    global askVolume
    global askOrderId
    global balanceCRY
    global sleepError
    global gotAskOrder
    global owners_warning
    global marketDecimals
    global minVolumeTrade
    global askOrderDetails
    global theorySellPrice

    #_____TRATAR DE CANCELAR ORDEN ASK SI LLEGASE A EXITIR UNA
    cancelAsk()
    
    #_____ACTUALIZAR SALDO CRYPTO EN LA CUENTA
    amountCRY = getCRYinAccount()
    
    #_____SI CANTIDAD DE CRYPTO SUPERA EL MÍNIMO VOLUMEN DE TRANSACCIÓN
    if round_decimals_down(askVolume,marketDecimals) >= minVolumeTrade:
        
        #_____SI EL SALDO DE CRYPTO SUPERA EL VOLUMEN IDEAL DE TRANSACCIÓN
        if (amountCRY >= askVolume):
            amountAsk = askVolume
            
            time.sleep(sleepApis)

            #_____MONTAR ORDEN ASK (LIMIT)
            while True:
                try:
                    orden = client.new_order(CRYPT.lower()+"-"+MONEY.lower(), "ask", "limit", amountAsk, limitAskPrice)
                    askOrderId = orden.id
                    break
                except Exception as e:
                    client = create_connection_buda()
                    print("[[ERROR]]: createAsk(limitAskPrice) - orden - ",e)
                    time.sleep(sleepApis)

            #_____TESTIGO DE CREACIÓN DE ORDEN
            gotAskOrder = True
            time.sleep(sleepApis)
            askOrderDetails = client.order_details(askOrderId)
            theorySellPrice = limitAskPrice                    
        
        #_____SI NO TENGO LAS CRYPTOS SUFICIENTES
        else:
            print("[[ERROR]]: createAsk(limitAskPrice) -> warning: no tengo los recursos suficientes")
            subject = "SpreadNet: WARNING"
            msg = "[[ERROR]]: createAsk(limitAskPrice) -> warning: no tengo los recursos suficientes<br><br>My Money: <b>${}</b><br><br>My Crypt: <b>{}</b>".format(round(getMONinAccount(),2),round(amountCRY,4))
            load_dotenv("spreadNet.env")
            owners_warning = os.getenv('owners_warning')
            owners_warning = json.loads(owners_warning)
            enviar_alerta(subject, msg, owners_warning)

    #_____PRINT
    print("MONTO ASK", amountAsk, limitAskPrice)

# CANCEL BID ORDER + WRITE BIGQUERY DATABSE
def cancelBid():

    global CRYPT
    global MONEY
    global client
    global sleepApis
    global sleepError
    global bidOrderId
    global gotBidOrder
    global client_surbtc
    global theoryBuyPrice
    global bidOrderDetails
    global theoryBuyExecuted

    time.sleep(sleepApis)

    #_____SI TENGO UNA ORDEN MONTADA (TENGO EL ID)
    if bidOrderId != None:

        #_____ACTUALIZAR DETALLES DE LA ORDEN
        while True:
            try:
                bidOrderDetails = client.order_details(bidOrderId)
                break
            except:
                print("[[ERROR]]: cancelBid() -> bidOrderDetails = client.order_details(bidOrderId)")
                time.sleep(sleepError)
        
        #_____SI LA ORDEN SE EJECUTÓ PARCIAL O TOTALMENTE
        if (bidOrderDetails.traded_amount.amount > 0.0):
            theoryBuyExecuted=theoryBuyPrice
            write_buy_sell_prices()
        
        #_____CANCELAR LA ORDEN A COMO DE LUGAR
        while True:
            time.sleep(sleepApis)
            try:
                client.cancel_order(bidOrderId)
                break
            except:
                print("[[ERROR]]: cancelBid() -> bidOrderDetails = client.cancel_order(bidOrderId)")
                time.sleep(sleepError)

        #_____ACTUALIZAR DETALLES DE LA ORDEN
        while True:

            time.sleep(sleepApis)

            while True:
                try:
                    bidOrderDetails = client.order_details(bidOrderId)
                    break
                except:
                    print("[[ERROR]]: cancelBid() -> bidOrderDetails = client.order_details(bidOrderId) (2)")
                    time.sleep(sleepError)

            #_____SI LA ORDEN YA SE MUESTRA COMO CANCELADA O TRANSADA
            if (bidOrderDetails.state=="canceled") or (bidOrderDetails.state=="traded"):
                break
            else:
                
                #_____CANCELAR LA ORDEN
                while True:
                    time.sleep(sleepApis)
                    try:
                        client.cancel_order(bidOrderId)
                        break
                    except:
                        print("[[ERROR]]: cancelBid() -> bidOrderDetails = client.cancel_order(bidOrderId) (2)")
        
        #_____SI LA ORDEN SE EJECUTÓ PARCIAL O TOTALMENTE
        if (bidOrderDetails.traded_amount.amount > 0.0):

            #_____CREAR CONEXIÓN CON BASE DE DATOS EN BIGQUERY
            os.environ["GOOGLE_APPLICATION_CREDENTIALS"]='gcp_json.json'
            bigquery_client=bigquery.Client(project="dogwood-terra-308100")
            dataset_ref=bigquery_client.dataset("spreadNet")
            table_ref=dataset_ref.table(CRYPT+"_"+MONEY)
            table=dataset_ref.table(CRYPT+"_"+MONEY)
            table=bigquery.Table(table)

            #_____ASIGNAR COLUMNAS A BASE DE DATOS DE LA ORDEN CANCELADA
            columns=["ID","UUID","MARKET_ID","ACCOUNT_ID","TYPE","STATE","CREATED_AT","FEE_CURRENCY","PRICE_TYPE","SOURCE","LIMIT","AMOUNT","ORIGINAL_AMOUNT","TRADED_AMOUNT","TOTAL_EXCHANGED","PAID_FEE"]
        
            #_____CREAR BASE DE DATOS PARA REGISTRO DE ORDEN
            append_order_dataframe=pd.DataFrame(bidOrderDetails.json).head(1)
            append_order_dataframe.columns=columns

            #_____QUITAR COLUMNAS INNECESARIAS
            append_order_dataframe=append_order_dataframe[["ID","ACCOUNT_ID","AMOUNT","CREATED_AT","FEE_CURRENCY","LIMIT","MARKET_ID","ORIGINAL_AMOUNT","PAID_FEE","PRICE_TYPE","STATE","TOTAL_EXCHANGED","TRADED_AMOUNT","TYPE"]]

            #_____DAR FORMATO A COLUMNAS
            append_order_dataframe.ID=append_order_dataframe.ID.astype(str)
            append_order_dataframe.ACCOUNT_ID=append_order_dataframe.ACCOUNT_ID.astype(str)
            append_order_dataframe.AMOUNT=append_order_dataframe.AMOUNT.astype(float)
            append_order_dataframe["CREATED_AT"]=pd.to_datetime(append_order_dataframe["CREATED_AT"])
            append_order_dataframe.FEE_CURRENCY=append_order_dataframe.FEE_CURRENCY.astype(str)
            append_order_dataframe.LIMIT=append_order_dataframe.LIMIT.astype(float)
            append_order_dataframe.MARKET_ID=append_order_dataframe.MARKET_ID.astype(str)
            append_order_dataframe.ORIGINAL_AMOUNT=append_order_dataframe.ORIGINAL_AMOUNT.astype(float)
            append_order_dataframe.PAID_FEE=append_order_dataframe.PAID_FEE.astype(float)
            append_order_dataframe.PRICE_TYPE=append_order_dataframe.PRICE_TYPE.astype(str)
            append_order_dataframe.STATE=append_order_dataframe.STATE.astype(str)
            append_order_dataframe.TOTAL_EXCHANGED=append_order_dataframe.TOTAL_EXCHANGED.astype(float)
            append_order_dataframe.TRADED_AMOUNT=append_order_dataframe.TRADED_AMOUNT.astype(float)
            append_order_dataframe.TYPE=append_order_dataframe.TYPE.astype(str)

            #_____AGREGAR COLUMNAS FALTANTES
            append_order_dataframe.at[0,"MY_CRYPTO"]=getCRYinAccount()
            append_order_dataframe.at[0,"MY_FIAT"]=getMONinAccount()
            append_order_dataframe.at[0,"MY_TRM"]=getFiatUsdQuote(MONEY)
            append_order_dataframe.at[0,"MY_CRYPTO_IN_FIAT"]=append_order_dataframe.at[0,"MY_CRYPTO"]*append_order_dataframe.at[0,"LIMIT"]
            append_order_dataframe.at[0,"MY_CRYPTO_IN_USD"]=append_order_dataframe.at[0,"MY_CRYPTO_IN_FIAT"]/append_order_dataframe.at[0,"MY_TRM"]
            append_order_dataframe.at[0,"MY_FIAT_IN_USD"]=append_order_dataframe.at[0,"MY_FIAT"]/append_order_dataframe.at[0,"MY_TRM"]
            append_order_dataframe.at[0,"MY_MARKET_USD"]=append_order_dataframe.at[0,"MY_CRYPTO_IN_USD"]+append_order_dataframe.at[0,"MY_FIAT_IN_USD"]
            append_order_dataframe.at[0,"ORIGINAL_AMOUNT_USD"]=append_order_dataframe.at[0,"ORIGINAL_AMOUNT"]*append_order_dataframe.at[0,"LIMIT"]/append_order_dataframe.at[0,"MY_TRM"]
            append_order_dataframe.at[0,"MY_EXECUTED_AMOUNT_USD"]=append_order_dataframe.at[0,"TOTAL_EXCHANGED"]/append_order_dataframe.at[0,"MY_TRM"]
            if append_order_dataframe.at[0,"TYPE"]=="Ask":
                append_order_dataframe.at[0,"MY_OPERATIONAL_UTILITY_FIAT"]=append_order_dataframe.at[0,"TOTAL_EXCHANGED"]
            else:
                append_order_dataframe.at[0,"MY_OPERATIONAL_UTILITY_FIAT"]=-append_order_dataframe.at[0,"TOTAL_EXCHANGED"]
            append_order_dataframe.at[0,"MY_OPERATIONAL_UTILITY_USD"]=append_order_dataframe.at[0,"MY_OPERATIONAL_UTILITY_FIAT"]/append_order_dataframe.at[0,"MY_TRM"]

            #_____DAR FORMATO A COLUMNAS NUEVAS
            append_order_dataframe.MY_CRYPTO=append_order_dataframe.MY_CRYPTO.astype(float)
            append_order_dataframe.MY_FIAT=append_order_dataframe.MY_FIAT.astype(float)
            append_order_dataframe.MY_TRM=append_order_dataframe.MY_TRM.astype(float)
            append_order_dataframe.MY_CRYPTO_IN_FIAT=append_order_dataframe.MY_CRYPTO_IN_FIAT.astype(float)
            append_order_dataframe.MY_CRYPTO_IN_USD=append_order_dataframe.MY_CRYPTO_IN_USD.astype(float)
            append_order_dataframe.MY_FIAT_IN_USD=append_order_dataframe.MY_FIAT_IN_USD.astype(float)
            append_order_dataframe.MY_MARKET_USD=append_order_dataframe.MY_MARKET_USD.astype(float)
            append_order_dataframe.ORIGINAL_AMOUNT_USD=append_order_dataframe.ORIGINAL_AMOUNT_USD.astype(float)
            append_order_dataframe.MY_EXECUTED_AMOUNT_USD=append_order_dataframe.MY_EXECUTED_AMOUNT_USD.astype(float)
            append_order_dataframe.MY_OPERATIONAL_UTILITY_FIAT=append_order_dataframe.MY_OPERATIONAL_UTILITY_FIAT.astype(float)
            append_order_dataframe.MY_OPERATIONAL_UTILITY_USD=append_order_dataframe.MY_OPERATIONAL_UTILITY_USD.astype(float)

            #_____SUBIR FILAS FALTANTES A GOOGLE CLOUD
            bigquery_client.insert_rows(bigquery_client.get_table(table_ref), append_order_dataframe.values.tolist())

        #_____ACTUALIZAR VARIABLES
        bidOrderDetails=None
        bidOrderId=None

    #_____SI NO TENGO ID DE LA ORDEN, PERO QUIERO TRATAR DE CANCELAR
    else:

        #_____PARAMETERS
        bidPendings=0
        bidOrderId=None
        bidOrderDetails=None

        time.sleep(sleepApis)
        
        while True:
            try:
                Market = client_surbtc.getMarket(CRYPT+"-"+MONEY)
                data=pd.DataFrame(Market.getPendingOrders())
                break
            except:
                print("[ERROR]: finishThemAll() -> client_surbtc")
                time.sleep(sleepError)

        #_____SI LEN DATA ES MAYOR A CERO
        if len(data)>0:

            bidPendings=len(data.loc[((data.state=="pending") | (data.state=="accepted")) & (data.type=="Bid")]) 

            #_____SI TENGO ÓRDENES EJECUTADAS PARCIAL O TOTALMENTE
            if (bidPendings>0):
                
                bidIdList=list(data.loc[((data.state=="pending") | (data.state=="accepted")) & (data.type=="Bid")].id.values)
            
                #_____SI TENGO BIDS PENDIENTES
                if len(bidIdList)>0:

                    for i in bidIdList:

                        time.sleep(sleepApis)
                        
                        #_____ACTUALIZAR DETALLES DE ÓRDENES
                        while True:
                            try:
                                bidOrderDetailsFinish = client.order_details(i)
                                break
                            except:
                                print("[ERROR]: finishThemAll() -> bidOrderDetailsFinish = client.order_details(i)")
                                time.sleep(sleepError)

                        #_____SI EJECUTÉ PARCIAL O TOTALEMTNE LA ÓRDEN -> GUARDAR PRECIOS
                        if (bidOrderDetailsFinish.traded_amount.amount > 0.0):
                            theoryBuyExecuted=theoryBuyPrice
                            write_buy_sell_prices()
                        
                        #_____ITERAR HASTA QUE SE CANCELE LA ORDEN
                        while True:
                            time.sleep(sleepApis)
                            try:
                                client.cancel_order(i)
                                break
                            except:
                                print("[ERROR]: finishThemAll() -> client.cancel_order(i)")
                                time.sleep(sleepError)

                        #_____ITERAR HASTA QUE SE ACTUALICE EL ESTADO DE LA ORDEN
                        while(True):

                            time.sleep(sleepApis)

                            while True:
                                try:
                                    bidOrderDetailsFinish = client.order_details(i)
                                    break
                                except:
                                    print("[ERROR]: finishThemAll() -> bidOrderDetailsFinish = client.order_details(i)(1)")
                                    time.sleep(sleepError)

                            #_____SI LA ORDEN YA SE ENCUENTRA CANCELADA -> SALIR DEL LOOP
                            if (bidOrderDetailsFinish.state=="canceled") or (bidOrderDetailsFinish.state=="traded"):
                                break
                            else:
                                while True:
                                    time.sleep(sleepApis)
                                    try:
                                        client.cancel_order(i)
                                        break
                                    except:
                                        print("[ERROR]: finishThemAll() -> bidOrderDetailsFinish = client.cancel_order(i)(2)")
                                        time.sleep(sleepError)

                        #_____SI LA ORDEN SE EJECUTÓ PARCIAL O TOTALMENTE
                        if (bidOrderDetails.traded_amount.amount > 0.0):

                            #_____CREAR CONEXIÓN CON BASE DE DATOS EN BIGQUERY
                            os.environ["GOOGLE_APPLICATION_CREDENTIALS"]='gcp_json.json'
                            bigquery_client=bigquery.Client(project="dogwood-terra-308100")
                            dataset_ref=bigquery_client.dataset("spreadNet")
                            table_ref=dataset_ref.table(CRYPT+"_"+MONEY)
                            table=dataset_ref.table(CRYPT+"_"+MONEY)
                            table=bigquery.Table(table)

                            #_____COLUMNAS
                            columns=["ID","UUID","MARKET_ID","ACCOUNT_ID","TYPE","STATE","CREATED_AT","FEE_CURRENCY","PRICE_TYPE","SOURCE","LIMIT","AMOUNT","ORIGINAL_AMOUNT","TRADED_AMOUNT","TOTAL_EXCHANGED","PAID_FEE"]
        
                            #_____CREAR BASE DE DATOS PARA REGISTRO DE ORDEN
                            append_order_dataframe=pd.DataFrame(askOrderDetails.json).head(1)
                            append_order_dataframe.columns=columns

                            #_____QUITAR COLUMNAS INNECESARIAS
                            append_order_dataframe=append_order_dataframe[["ID","ACCOUNT_ID","AMOUNT","CREATED_AT","FEE_CURRENCY","LIMIT","MARKET_ID","ORIGINAL_AMOUNT","PAID_FEE","PRICE_TYPE","STATE","TOTAL_EXCHANGED","TRADED_AMOUNT","TYPE"]]

                            #_____DAR FORMATO A COLUMNAS
                            append_order_dataframe.ID=append_order_dataframe.ID.astype(str)
                            append_order_dataframe.ACCOUNT_ID=append_order_dataframe.ACCOUNT_ID.astype(str)
                            append_order_dataframe.AMOUNT=append_order_dataframe.AMOUNT.astype(float)
                            append_order_dataframe["CREATED_AT"]=pd.to_datetime(append_order_dataframe["CREATED_AT"])
                            append_order_dataframe.FEE_CURRENCY=append_order_dataframe.FEE_CURRENCY.astype(str)
                            append_order_dataframe.LIMIT=append_order_dataframe.LIMIT.astype(float)
                            append_order_dataframe.MARKET_ID=append_order_dataframe.MARKET_ID.astype(str)
                            append_order_dataframe.ORIGINAL_AMOUNT=append_order_dataframe.ORIGINAL_AMOUNT.astype(float)
                            append_order_dataframe.PAID_FEE=append_order_dataframe.PAID_FEE.astype(float)
                            append_order_dataframe.PRICE_TYPE=append_order_dataframe.PRICE_TYPE.astype(str)
                            append_order_dataframe.STATE=append_order_dataframe.STATE.astype(str)
                            append_order_dataframe.TOTAL_EXCHANGED=append_order_dataframe.TOTAL_EXCHANGED.astype(float)
                            append_order_dataframe.TRADED_AMOUNT=append_order_dataframe.TRADED_AMOUNT.astype(float)
                            append_order_dataframe.TYPE=append_order_dataframe.TYPE.astype(str)

                            #_____AGREGAR COLUMNAS FALTANTES
                            append_order_dataframe.at[0,"MY_CRYPTO"]=getCRYinAccount()
                            append_order_dataframe.at[0,"MY_FIAT"]=getMONinAccount()
                            append_order_dataframe.at[0,"MY_TRM"]=getFiatUsdQuote(MONEY)
                            append_order_dataframe.at[0,"MY_CRYPTO_IN_FIAT"]=append_order_dataframe.at[0,"MY_CRYPTO"]*append_order_dataframe.at[0,"LIMIT"]
                            append_order_dataframe.at[0,"MY_CRYPTO_IN_USD"]=append_order_dataframe.at[0,"MY_CRYPTO_IN_FIAT"]/append_order_dataframe.at[0,"MY_TRM"]
                            append_order_dataframe.at[0,"MY_FIAT_IN_USD"]=append_order_dataframe.at[0,"MY_FIAT"]/append_order_dataframe.at[0,"MY_TRM"]
                            append_order_dataframe.at[0,"MY_MARKET_USD"]=append_order_dataframe.at[0,"MY_CRYPTO_IN_USD"]+append_order_dataframe.at[0,"MY_FIAT_IN_USD"]
                            append_order_dataframe.at[0,"ORIGINAL_AMOUNT_USD"]=(append_order_dataframe.at[0,"ORIGINAL_AMOUNT"]*append_order_dataframe.at[0,"LIMIT"])/append_order_dataframe.at[0,"MY_TRM"]
                            append_order_dataframe.at[0,"MY_EXECUTED_AMOUNT_USD"]=append_order_dataframe.at[0,"TOTAL_EXCHANGED"]/append_order_dataframe.at[0,"MY_TRM"]
                            if append_order_dataframe.at[0,"TYPE"]=="Ask":
                                append_order_dataframe.at[0,"MY_OPERATIONAL_UTILITY_FIAT"]=append_order_dataframe.at[0,"TOTAL_EXCHANGED"]
                            else:
                                append_order_dataframe.at[0,"MY_OPERATIONAL_UTILITY_FIAT"]=-append_order_dataframe.at[0,"TOTAL_EXCHANGED"]
                            append_order_dataframe.at[0,"MY_OPERATIONAL_UTILITY_USD"]=append_order_dataframe.at[0,"MY_OPERATIONAL_UTILITY_FIAT"]/append_order_dataframe.at[0,"MY_TRM"]

                            #_____DAR FORMATO A COLUMNAS NUEVAS
                            append_order_dataframe.MY_CRYPTO=append_order_dataframe.MY_CRYPTO.astype(float)
                            append_order_dataframe.MY_FIAT=append_order_dataframe.MY_FIAT.astype(float)
                            append_order_dataframe.MY_TRM=append_order_dataframe.MY_TRM.astype(float)
                            append_order_dataframe.MY_CRYPTO_IN_FIAT=append_order_dataframe.MY_CRYPTO_IN_FIAT.astype(float)
                            append_order_dataframe.MY_CRYPTO_IN_USD=append_order_dataframe.MY_CRYPTO_IN_USD.astype(float)
                            append_order_dataframe.MY_FIAT_IN_USD=append_order_dataframe.MY_FIAT_IN_USD.astype(float)
                            append_order_dataframe.MY_MARKET_USD=append_order_dataframe.MY_MARKET_USD.astype(float)
                            append_order_dataframe.ORIGINAL_AMOUNT_USD=append_order_dataframe.ORIGINAL_AMOUNT_USD.astype(float)
                            append_order_dataframe.MY_EXECUTED_AMOUNT_USD=append_order_dataframe.MY_EXECUTED_AMOUNT_USD.astype(float)
                            append_order_dataframe.MY_OPERATIONAL_UTILITY_FIAT=append_order_dataframe.MY_OPERATIONAL_UTILITY_FIAT.astype(float)
                            append_order_dataframe.MY_OPERATIONAL_UTILITY_USD=append_order_dataframe.MY_OPERATIONAL_UTILITY_USD.astype(float)

                            #_____SUBIR FILAS FALTANTES A GOOGLE CLOUD
                            bigquery_client.insert_rows(bigquery_client.get_table(table_ref), append_order_dataframe.values.tolist())

    #_____PRINT
    print("CANCELO BID")

# CREAR ORDEN BID (LIMIT)
def createBid(limitBidPrice):
    
    global MONEY
    global CRYPT
    global client
    global pastBids
    global pastBids
    global sleepApis
    global bidVolume
    global bidOrderId
    global balanceCRY
    global sleepError
    global gotBidOrder
    global theoryBuyPrice
    global owners_warning
    global marketDecimals
    global minVolumeTrade
    global bidOrderDetails

    #_____TRATAR DE CANCELAR ORDEN BID SI LLEGASE A EXITIR UNA
    cancelBid()
    
    #_____ACTUALIZAR SALDO CRYPTO EN LA CUENTA
    myActualMoney = getMONinAccount()

    #_____SIMULAR COMPRA CON PLATA QUE TENGO
    while True:
        try:
            buyQuotation=client.quotation_market(amount=myActualMoney,quotation_type="bid_given_value",market_id=CRYPT.lower()+"-"+MONEY.lower())
            buyQuotation=buyQuotation.base_balance_change[0]
            break
        except:
            print("[ERROR]: createBid(limitBidPrice) - simulation")
            time.sleep(sleepError)
    
    #_____SI CANTIDAD DE CRYPTO SUPERA EL MÍNIMO VOLUMEN DE TRANSACCIÓN
    if round_decimals_down(bidVolume,marketDecimals) >= minVolumeTrade:
        
        #_____SI EL SALDO DE CRYPTO SUPERA EL VOLUMEN IDEAL DE TRANSACCIÓN
        if (buyQuotation >= bidVolume):
            amountBid = bidVolume

            time.sleep(sleepApis)

            #_____MONTAR ORDEN BID (LIMIT)
            while True:
                try:
                    orden = client.new_order(CRYPT.lower()+"-"+MONEY.lower(), "bid", "limit", amountBid, limitBidPrice)
                    bidOrderId = orden.id
                    break
                except Exception as e:
                    client = create_connection_buda()
                    print("[[ERROR]]: createBid(limitBidPrice) -> orden - ",e)
                    time.sleep(sleepApis)

            #_____TESTIGO DE CREACIÓN DE ORDEN
            gotBidOrder = True
            time.sleep(sleepApis)
            bidOrderDetails = client.order_details(bidOrderId)
            theoryBuyPrice=limitBidPrice
        
        #_____SI NO TENGO LAS CRYPTOS SUFICIENTES
        else:
            print("[[ERROR]]: createBid(limitBidPrice) -> warning: no tengo los recursos suficientes")
            subject = "SpreadNet: WARNING"
            msg = "[[ERROR]]: createBid(limitBidPrice) -> warning: no tengo los recursos suficientes<br><br>My Money: <b>${}</b><br><br>My Crypt: <b>{}</b>".format(round(getMONinAccount(),2),round(buyQuotation,4))
            load_dotenv("spreadNet.env")
            owners_warning = os.getenv('owners_warning')
            owners_warning = json.loads(owners_warning)
            enviar_alerta(subject, msg, owners_warning)

    #_____PRINT
    print("MONTO BID", amountBid, limitBidPrice)

#_____INICIALIZAR
client=None

askOrderId=""
bidOrderId=""

askVolume = 0.0
bidVolume = 0.0

newAskPrice = 0.0
newBidPrice = 0.0

gotAskOrder = False
gotBidOrder = False

limitAskPrice = 0.0
limitBidPrice = 0.0

theoryBuyPrice=0.0
theoryBuyExecuted=0.0

newLimitAskVolume = 0.0
newLimitBidVolume = 0.0

theorySellPrice=0.0
theorySellExecuted=0.0

newPreLimitAskPrice = 0.0
newPreLimitBidPrice = 0.0

#_____VARIABLES DE ENTORNO
load_dotenv("spreadNet.env")

API_KEY=os.getenv('API_KEY')
API_SECRET=os.getenv('API_SECRET')

me=os.getenv('me')
password=os.getenv('password')

URL="https://www.buda.com/api/v2/markets/"+CRYPT.lower()+"-"+MONEY.lower()+"/order_book.json"

#_____TRY DE CONTINGENCIA
try:

    #_____SI MERCADO SE ENCUENTRA ABIERTO
    if getOnOffMarket(CRYPT,MONEY)==1:
        
        #_____CLIENTE BUDA
        client = create_connection_buda()

        #_____CLIENTE SURBTC
        client_surbtc = surbtc.Client(API_KEY,API_SECRET)

        #_____BALANCEAR
        balancing_Ask_Bid()
        
        time.sleep(sleepApis)
        
        #_____PRINT
        print("OPERANDO")

        #_____CANCELAR CUALQUIER TIPO DE ORDEN EXISTENTE + BALANCEO DE ASK BID + ACTUALIZAR FIAT + CRYPTO DISPONIBLES
        finishThemAll()

        #_____ACTUALIZAR THEORY PRICES EXECUTED
        read_buy_sell_prices()
        
        #_____SI SE ESTÁ EN ESCENARIO ASK
        if (askVolume>0.0) and (bidVolume==0.0):

            #_____PRINT
            print("ENTRO ASK")
            
            #_____ACTUALIZAR LIBRO DE ORDENES + DETERMIAR EL PRECIO TEORICO TRANSACCION
            updateLimits()

            #_____DETERMINAR SI HAY SPREAD
            utilityMargin=validMargin(limitAskPrice, theoryBuyExecuted)

            #_____SI HAY SPREAD SUFICIENTE
            if utilityMargin > utilityMarginThreshold:
            
                #_____MONTAR POSICION ASK (LIMIT)
                createAsk(limitAskPrice)

                #_____LOOP HASTE QUE EJECUTO ASK
                while gotAskOrder:

                    time.sleep(sleepErrorApis)
                    
                    #_____ACTUALIZAR ORDEN MONTADA
                    while True:
                        try:
                            askOrderDetails = client.order_details(askOrderId)
                            break
                        except:
                            time.sleep(sleepError)

                    #_____REVISAR SI SE EJECUTÓ ORDEN ASK PARCIAL O TOTALMENTE
                    if (askOrderDetails.traded_amount.amount > 0.0):
                    
                        #_____ESCRIBIR PRECIOS EJECUTADOS + CANCELAR ORDEN + BALANCEAR
                        cancelAsk()
                        balancing_Ask_Bid()
                        gotAskOrder = False
                    
                    #_____ELSE NO ME EJECUTARON ASK
                    else:
                    
                        #_____ACTUALIZAR LIBRO DE ORDENES (PRECIOS + VOLUMENES PUNTA)
                        updatePriceVolume()

                        #####

                        #_____SI HUBO CAMBIOS DEL MERCADO: PRECIO PUNTA CAMBIA + REDUZCO SPREAD INNECESARIAMENTE + SE MONTARON CAMBIANDO LA PROPORCIÓN
                        if (newAskPrice != theorySellPrice) or \
                            ((theorySellPrice<=newPreLimitAskPrice) and (newAskPrice == theorySellPrice)) or \
                            ((float(askVolume/(newLimitAskVolume))<tradeProportion) and (newAskPrice == theorySellPrice)):

                            #_____CANCELAR ASK
                            cancelAsk()
                            balancing_Ask_Bid()
                            gotAskOrder = False

                            #_____SI SE ESTÁ EN ESCENARIO ASK
                            if (askVolume>0.0) and (bidVolume==0.0):
                                
                                time.sleep(sleepErrorApis)

                                #_____ACTUALIZAR LIBRO DE ORDENES + DETERMIAR EL PRECIO TEORICO TRANSACCION
                                updateLimits()
                                
                                #_____MONTAR POSICION ASK (LIMIT)
                                createAsk(limitAskPrice)

        #####
        
        #_____ACTUALIZAR THEORY PRICES EXECUTED
        read_buy_sell_prices()

        #_____ELIF SI SE ESTÁ EN ESCENARIO BID
        if (askVolume==0.0) and (bidVolume>0.0):

            #_____PRINT
            print("ENTRO BID")
            
            #_____ACTUALIZAR LIBRO DE ORDENES + DETERMIAR EL PRECIO TEORICO TRANSACCION
            updateLimits()

            #_____DETERMINAR SI HAY SPREAD
            utilityMargin=validMargin(theorySellExecuted, limitBidPrice)

            #_____SI HAY SPREAD SUFICIENTE
            if utilityMargin > utilityMarginThreshold:
            
                #_____MONTAR POSICION BID (LIMIT)
                createBid(limitBidPrice)
                
                #_____LOOP HASTE QUE EJECUTO BID
                while gotBidOrder:

                    time.sleep(sleepErrorApis)
                    
                    #_____ACTUALIZAR ORDEN MONTADA
                    while True:
                        try:
                            bidOrderDetails = client.order_details(bidOrderId)
                            break
                        except:
                            time.sleep(sleepError)

                    #_____REVISAR SI SE EJECUTÓ ORDEN BID PARCIAL O TOTALMENTE
                    if (bidOrderDetails.traded_amount.amount > 0.0):
                    
                        #_____ESCRIBIR PRECIOS EJECUTADOS + CANCELAR ORDEN + BALANCEAR
                        cancelBid()
                        balancing_Ask_Bid()
                        gotBidOrder = False
                    
                    #_____ELSE NO ME EJECUTARON BID
                    else:
                    
                        #_____ACTUALIZAR LIBRO DE ORDENES (PRECIOS + VOLUMENES PUNTA)
                        updatePriceVolume()

                        #####

                        #_____SI HUBO CAMBIOS DEL MERCADO: PRECIO PUNTA CAMBIA + REDUZCO SPREAD INNECESARIAMENTE + SE MONTARON CAMBIANDO LA PROPORCIÓN
                        if (newBidPrice != theoryBuyPrice) or \
                            ((theoryBuyPrice>=newPreLimitBidPrice) and (newBidPrice == theoryBuyPrice)) or \
                            ((float(bidVolume/(newLimitBidVolume))<tradeProportion) and (newBidPrice == theoryBuyPrice)):

                            #_____CANCELAR BID
                            cancelBid()
                            balancing_Ask_Bid()
                            gotBidOrder = False

                            #_____SI SE ESTÁ EN ESCENARIO BID
                            if (askVolume==0.0) and (bidVolume>0.0):

                                time.sleep(sleepErrorApis)

                                #_____ACTUALIZAR LIBRO DE ORDENES + DETERMIAR EL PRECIO TEORICO TRANSACCION
                                updateLimits()
                                
                                #_____MONTAR POSICION ASK (LIMIT)
                                createBid(limitBidPrice)

        #####
        
        #_____ACTUALIZAR THEORY PRICES EXECUTED
        read_buy_sell_prices()
        
        #_____ELIF SI SE ESTÁ EN ESCENARIO ASK + BID
        if (askVolume>0.0) and (bidVolume>0.0):

            #_____PRINT
            print("ENTRO ASK BID")

            #_____ACTUALIZAR LIBRO DE ORDENES + DETERMIAR EL PRECIO TEORICO TRANSACCION
            updateLimits()

            #_____DETERMINAR SI HAY SPREAD
            utilityMargin=validMargin(limitAskPrice, limitBidPrice)

            #_____SI HAY SPREAD SUFICIENTE
            if utilityMargin > utilityMarginThreshold:

                #_____MONTO POSICION LIMIT ASK
                createAsk(limitAskPrice)

                #_____COLOCO POSICION LIMIT BID
                createBid(limitBidPrice)

                #_____LOOP HASTE QUE EJECUTO ASK O BID
                while (gotAskOrder or gotBidOrder):

                    time.sleep(sleepErrorApis)

                    #_____ACTUALIZAR ORDEN ASK MONTADA
                    while True:
                        try:
                            askOrderDetails = client.order_details(askOrderId)
                            break
                        except:
                            time.sleep(sleepError)

                    #_____ACTUALIZAR ORDEN BID MONTADA
                    while True:
                        try:
                            bidOrderDetails = client.order_details(bidOrderId)
                            break
                        except:
                            time.sleep(sleepError)

                    #####

                    #_____(SI ME EJECUTARON ASK) O (SI ME EJECUTARON BID)
                    if (askOrderDetails.traded_amount.amount > 0.0) or (bidOrderDetails.traded_amount.amount > 0.0):

                        #_____ESCRIBIR PRECIOS EJECUTADOS + CANCELAR ORDEN (ASK)
                        cancelAsk()
                        gotAskOrder = False

                        #_____ESCRIBIR PRECIOS EJECUTADOS + CANCELAR ORDEN + BALANCEAR
                        cancelBid()
                        gotBidOrder = False

                        time.sleep(sleepApis)

                        #_____BALANCEAR
                        balancing_Ask_Bid()

                    #####

                    #_____ELSE NO ME EJECUTARON ASK NI BID
                    else:

                        #_____ACTUALIZAR LIBRO DE ORDENES (PRECIOS + VOLUMENES PUNTA)
                        updatePriceVolume()

                        #_____SI HUBO CAMBIOS DEL MERCADO: PRECIO PUNTA CAMBIA + REDUZCO SPREAD INNECESARIAMENTE + SE MONTARON CAMBIANDO LA PROPORCIÓN
                        if((newAskPrice != theorySellPrice) or \
                        ((theorySellPrice<=newPreLimitAskPrice) and (newAskPrice == theorySellPrice)) or \
                            ((float(askVolume/(newLimitAskVolume))<tradeProportion) and (newAskPrice == theorySellPrice))) or \
                            ((newBidPrice != theoryBuyPrice) or \
                                ((theoryBuyPrice>=newPreLimitBidPrice) and (newBidPrice == theoryBuyPrice)) or \
                                ((float(bidVolume/(newLimitBidVolume))<tradeProportion) and (newBidPrice == theoryBuyPrice))):

                            #_____ESCRIBIR PRECIOS EJECUTADOS + CANCELAR ORDEN (ASK)
                            cancelAsk()
                            gotAskOrder = False

                            #_____ESCRIBIR PRECIOS EJECUTADOS + CANCELAR ORDEN + BALANCEAR
                            cancelBid()
                            gotBidOrder = False

                            time.sleep(sleepApis)

                            #_____BALANCEAR
                            balancing_Ask_Bid()

                            #_____ELIF SI SE ESTÁ EN ESCENARIO ASK + BID
                            if (askVolume>0.0) and (bidVolume>0.0):
                                
                                time.sleep(sleepErrorApis)

                                #_____ACTUALIZAR LIBRO DE ORDENES + DETERMIAR EL PRECIO TEORICO TRANSACCION
                                updateLimits()

                                #_____DETERMINAR SI HAY SPREAD
                                utilityMargin=validMargin(limitAskPrice, limitBidPrice)

                                #_____SI HAY SPREAD SUFICIENTE
                                if utilityMargin > utilityMarginThreshold:

                                    #_____MONTO POSICION LIMIT ASK
                                    createAsk(limitAskPrice)

                                    #_____COLOCO POSICION LIMIT BID
                                    createBid(limitBidPrice)

except Exception as e:
    
    finishThemAll()
    # shutDownMarket(CRYPT,MONEY)
    # subject = "SpreadNet: SHUTDOWN"
    # msg = "MARKET: {} is OFF <br><br> {}".format(MONEY.upper()+"_"+CRYPT.upper(),e)
    # load_dotenv("spreadNet.env")
    # owners_warning = os.getenv('owners_warning')
    # owners_warning = json.loads(owners_warning)
    # enviar_alerta(subject, msg, owners_warning)
    print(e)

#_____ESPERA POR ERROR
time.sleep(60)