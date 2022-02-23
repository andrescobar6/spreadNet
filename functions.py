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
    return math.floor(number * factor) / factorho,

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

    me = get_config('owner_information','ME')
    password = get_config('owner_information','PASSWORD')

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
    global balanceCRY
    global sleepError
    
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
    global sleepError
    global balanceMON
    
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
    return float(fiatQuotes.loc[fiatQuotes.MARKET == "USD"+fiat.upper()].QUOTES[0])

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
            database_past_asks_bids=gbq.read_gbq("SELECT * FROM [dogwood-terra-308100:spreadNet."+CRYPT+"_"+MONEY+"] ORDER BY CREATED_AT DESC",project_id="dogwood-terra-308100",dialect="legacy")
            break
        except:
            print("[ERROR]: updatePast_Asks_Bids()")
            time.sleep(sleepError)
            
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
    database_past_asks_bids.MY_CRYPTO_IN_FIAT=database_past_asks_bids.MY_CRYPTO_IN_FIAT.astype(float)
    database_past_asks_bids.MY_CRYPTO_IN_USD=database_past_asks_bids.MY_CRYPTO_IN_USD.astype(float)
    database_past_asks_bids.MY_FIAT_IN_USD=database_past_asks_bids.MY_FIAT_IN_USD.astype(float)
    database_past_asks_bids.MY_MARKET_USD=database_past_asks_bids.MY_MARKET_USD.astype(float)
    database_past_asks_bids.MY_EXECUTED_AMOUNT=database_past_asks_bids.MY_EXECUTED_AMOUNT.astype(float)
    database_past_asks_bids.MY_OPERATIONAL_UTILITY=database_past_asks_bids.MY_OPERATIONAL_UTILITY.astype(float)

    #_____

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
    global buyPrice
    global pastBids
    global sellPrice
    global sleepError
    global minVolumeTrade
    global maxTradingVolumeProportion
    
    updatePast_Asks_Bids()
    
    if abs(pastAsks-pastBids)<minVolumeTrade:
        
        sellPrice=0.0
        buyPrice=0.0
        
        #______
    
        myActualMoney=getMONinAccount()
        myActualCrypt=getCRYinAccount()
        recomendedVolume=myActualCrypt*maxTradingVolumeProportion

        #_____SIMULAR MARKET ORDER CON EL RECOMENDED VOLUME QUE TENGO
        
        while True:
            try:
                sellQuotation=client.quotation_market(amount=myActualMoney,quotation_type="bid_given_value",market_id=CRYPT.lower()+"-"+MONEY.lower())
                break
            except:
                print("[ERROR]: history_trades()")
                time.sleep(sleepError)

        sellQuotation=myBudaOrder.base_balance_change[0]

        #_____ 

        if recomendedVolume>sellQuotation*maxTradingVolumeProportion:
            recomendedVolume=sellQuotation*maxTradingVolumeProportion
                
    else:
        recomendedVolume=abs(pastAsks-pastBids)

    return round_decimals_down(recomendedVolume,5)

# ACTUALIZO BASE DE DATOS + BALANCEO + ACTUALIZAR FIAT + CRYPTO DISPONIBLES
def balancing_Ask_Bid():
    
    global volume
    global pastAsks
    global pastBids
    global askVolume
    global bidVolume
    global askControl
    global bidControl
    global owners_warning
    
    volume=history_trades()
    controlVariable=abs(pastAsks-pastBids)  
    
    if abs(pastAsks-pastBids)<=minVolumeTrade:
        askVolume=volume
        bidVolume=volume
    elif pastAsks<pastBids:
        if (round_decimals_down(pastBids-pastAsks,5)>volume):
            subject = "SpreadNet: WARNING"
            msg = "[[ERROR]]: balancing_Ask_Bid() -> if (round_decimals_down(pastBids-pastAsks,5)>volume)"
            owners = json.loads(config.get('owner_information','OWNERS_ALERT'))
            enviar_alerta(subject, msg, owners_warning)
        else:
            askVolume=round_decimals_down(pastBids-pastAsks,5) if (pastBids-pastAsks>=minVolumeTrade) else 0.0
            bidVolume=0.0
    elif pastAsks>pastBids:
        if (round_decimals_down(pastAsks-pastBids,5)>volume):
            subject = "SpreadNet: WARNING"
            msg = "[[ERROR]]: balancing_Ask_Bid() -> if (round_decimals_down(pastBids-pastAsks,5)>volume)"
            owners = json.loads(config.get('owner_information','OWNERS_ALERT'))
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
    global last_volume_traded_fee

    #_____CREATE DICT
    last_buy_sell_prices_dic = {"theorySellExecuted": theorySellExecuted,
                                "theoryBuyExecuted": theoryBuyExecuted,
                                "last_volume_traded_fee":last_volume_traded_fee}

    #_____CREAR CONEXIÓN CON GOOGLE CLOUD
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"]="gcp_json.json"
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_or_name="marketmaker")
    
    #_____UPLOAD FILE
    blob=bucket.blob("last_buy_sell_prices_dic_"+CRYPT+"_"+MONEY+".txt")
    blob.upload_from_string(data=str(last_buy_sell_prices_dic))

# ELIMINA TODAS LAS ORDENES EXISTENTES + ESCRIBO ODERNES EJECUTADAS
def finishThemAll():

    global MONEY
    global CRYPT
    global client
    global API_KEY
    global sleepApi
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
    global last_volume_traded_fee
    global database_past_asks_bids

    #_____PARAMETERS
    askPendings=0
    bidPendings=0

    askOrderId=None
    askOrderDetails=None

    bidOrderId=None
    bidOrderDetails=None
    
    Market = client_surbtc.getMarket(CRYPT+"-"+MONEY)
    data=pd.DataFrame(Market.getPendingOrders())
    
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
                        try:
                            client.cancel_order(i)
                            break
                        except:
                            print("[ERROR]: finishThemAll() -> client.cancel_order(i)")
                            time.sleep(sleepError)

                    #_____ITERAR HASTA QUE SE ACTUALICE EL ESTADO DE LA ORDEN
                    while(True):

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
                                try:
                                    client.cancel_order(i)
                                    break
                                except:
                                    print("[ERROR]: finishThemAll() -> askOrderDetailsFinish = client.cancel_order(i)(2)")
                                    time.sleep(sleepError)

                    

                    #_____CREAR CONEXIÓN CON BASE DE DATOS EN BIGQUERY
                    bigquery_client=bigquery.Client(project="dogwood-terra-308100")
                    dataset_ref=bigquery_client.dataset("spreadNet")
                    table_ref=dataset_ref.table(CRYPT+"_"+MONEY)
                    table=dataset_ref.table(CRYPT+"_"+MONEY)
                    table=bigquery.Table(table)

                    #_____ASIGNAR COLUMNAS A BASE DE DATOS DE LA ORDEN CANCELADA
                    columns=["ID","ACCOUNT_ID","AMOUNT","CREATED_AT","FEE_CURRENCY","LIMIT","MARKET_ID","ORIGINAL_AMOUNT","PAID_FEE","PRICE_TYPE","STATE","TOTAL_EXCHANGED","TRADED_AMOUNT","TYPE","JSON"]
                    append_order_dataframe=pd.DataFrame(columns=columns)

                    #_____CREAR BASE DE DATOS PARA REGISTRO DE ORDEN
                    order_dataframe=pd.DataFrame(askOrderDetailsFinish[14]).head(1)
                    append_order_dataframe=append_order_dataframe.append(order_dataframe,sort=False)

                    #_____QUITAR COLUMNAS INNECESARIAS
                    append_order_dataframe=append_order_dataframe[["ID","ACCOUNT_ID","AMOUNT","CREATED_AT","FEE_CURRENCY","LIMIT","MARKET_ID","ORIGINAL_AMOUNT","PAID_FEE","PRICE_TYPE","STATE","TOTAL_EXCHANGED","TRADED_AMOUNT","TYPE"]]

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

                    #_____SUBIR FILAS FALTANTES A GOOGLE CLOUD
                    bigquery_client.insert_rows(bigquery_client.get_table(table_ref), append_order_dataframe.values.tolist())

            #_____SI TENGO BIDS PENDIENTES
            if len(bidIdList)>0:

                for i in bidIdList:
                    
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
                        theorySellExecuted=theorySellPrice
                        write_buy_sell_prices()
                    
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
                                try:
                                    client.cancel_order(i)
                                    break
                                except:
                                    print("[ERROR]: finishThemAll() -> bidOrderDetailsFinish = client.cancel_order(i)(2)")
                                    time.sleep(sleepError)

                    

                    #_____CREAR CONEXIÓN CON BASE DE DATOS EN BIGQUERY
                    bigquery_client=bigquery.Client(project="dogwood-terra-308100")
                    dataset_ref=bigquery_client.dataset("spreadNet")
                    table_ref=dataset_ref.table(CRYPT+"_"+MONEY)
                    table=dataset_ref.table(CRYPT+"_"+MONEY)
                    table=bigquery.Table(table)

                    #_____ASIGNAR COLUMNAS A BASE DE DATOS DE LA ORDEN CANCELADA
                    columns=["ID","ACCOUNT_ID","AMOUNT","CREATED_AT","FEE_CURRENCY","LIMIT","MARKET_ID","ORIGINAL_AMOUNT","PAID_FEE","PRICE_TYPE","STATE","TOTAL_EXCHANGED","TRADED_AMOUNT","TYPE","JSON"]
                    append_order_dataframe=pd.DataFrame(columns=columns)

                    #_____CREAR BASE DE DATOS PARA REGISTRO DE ORDEN
                    order_dataframe=pd.DataFrame(bidOrderDetailsFinish[14]).head(1)
                    append_order_dataframe=append_order_dataframe.append(order_dataframe,sort=False)

                    #_____QUITAR COLUMNAS INNECESARIAS
                    append_order_dataframe=append_order_dataframe[["ID","ACCOUNT_ID","AMOUNT","CREATED_AT","FEE_CURRENCY","LIMIT","MARKET_ID","ORIGINAL_AMOUNT","PAID_FEE","PRICE_TYPE","STATE","TOTAL_EXCHANGED","TRADED_AMOUNT","TYPE"]]

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
                    if append_order_dataframe.at[0,"TYPE"]=="Bid":
                        append_order_dataframe.at[0,"MY_OPERATIONAL_UTILITY_FIAT"]=append_order_dataframe.at[0,"TOTAL_EXCHANGED"]
                    else:
                        append_order_dataframe.at[0,"MY_OPERATIONAL_UTILITY_FIAT"]=-append_order_dataframe.at[0,"TOTAL_EXCHANGED"]
                    append_order_dataframe.at[0,"MY_OPERATIONAL_UTILITY_USD"]=append_order_dataframe.at[0,"MY_OPERATIONAL_UTILITY_FIAT"]/append_order_dataframe.at[0,"MY_TRM"]

                    #_____SUBIR FILAS FALTANTES A GOOGLE CLOUD
                    bigquery_client.insert_rows(bigquery_client.get_table(table_ref), append_order_dataframe.values.tolist())

        balancing_Ask_Bid()

# DESCARGAR ORDERBOOK DE BUDA
def request_order_book():
    
    global URL
    global API_KEY
    global API_SECRET
    
    while True:
        try:
            with requests.get(URL, auth=BudaHMACAuth(API_KEY, API_SECRET)) as r:
                order_book = r.json()
                if ( order_book != None and 'order_book' in order_book ):
                    return order_book['order_book']
                    break
                else:
                    print('[[ERROR]]: request_order_book()')
        except:
            print('[[ERROR]]: request_order_book()')

# ACTUALIZAR PRECIOS LÍMITES DE LIBRO DE ÓRDENES + ACTUALIZAR TOPES
def updateLimits():
    
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
    global margin
    global commission
    return ((limitAskPrice*(1-2*commission))/limitBidPrice)-1 > margin

# CANCEL ASK ORDER + WRITE BIGQUERY DATABSE
def cancelAsk():

    global CRYPT
    global MONEY
    global client
    global askOrderId
    global gotAskOrder
    global theorySellPrice
    global askOrderDetails
    global theorySellExecuted
    
    #_____ACTUALIZAR DETALLES DE LA ORDEN
    while True:
        try:
            askOrderDetails = client.order_details(askOrderId)
            break
        except:
            print("[[ERROR]]: cancelAsk() -> askOrderDetails = client.order_details(askOrderId)")
    
    #_____SI LA ORDEN SE EJECUTÓ PARCIAL O TOTALMENTE
    if (askOrderDetails.traded_amount.amount > 0.0):
        theorySellExecuted=theorySellPrice
        write_buy_sell_prices()
    
    #_____CANCELAR LA ORDEN A COMO DE LUGAR
    while True:
        try:
            client.cancel_order(askOrderId)
            break
        except:
            print("[[ERROR]]: cancelAsk() -> askOrderDetails = client.cancel_order(askOrderId)")
    
    #_____ACTUALIZAR DETALLES DE LA ORDEN
    while True:
        while True:
            try:
                askOrderDetails = client.order_details(askOrderId)
                break
            except:
                print("[[ERROR]]: cancelAsk() -> askOrderDetails = client.order_details(askOrderId) (2)")

        #_____SI LA ORDEN YA SE MUESTRA COMO CANCELADA O TRANSADA
        if (askOrderDetails.state=="canceled") or (askOrderDetails.state=="traded"):
            break
        else:
            
            #_____CANCELAR LA ORDEN
            while True:
                try:
                    client.cancel_order(askOrderId)
                    break
                except:
                    print("[[ERROR]]: cancelAsk() -> askOrderDetails = client.cancel_order(askOrderId) (2)")
    
    #_____CREAR CONEXIÓN CON BASE DE DATOS EN BIGQUERY
    bigquery_client=bigquery.Client(project="dogwood-terra-308100")
    dataset_ref=bigquery_client.dataset("spreadNet")
    table_ref=dataset_ref.table(CRYPT+"_"+MONEY)
    table=dataset_ref.table(CRYPT+"_"+MONEY)
    table=bigquery.Table(table)

    #_____ASIGNAR COLUMNAS A BASE DE DATOS DE LA ORDEN CANCELADA
    columns=["ID","ACCOUNT_ID","AMOUNT","CREATED_AT","FEE_CURRENCY","LIMIT","MARKET_ID","ORIGINAL_AMOUNT","PAID_FEE","PRICE_TYPE","STATE","TOTAL_EXCHANGED","TRADED_AMOUNT","TYPE","JSON"]
    append_order_dataframe=pd.DataFrame(columns=columns)

    #_____CREAR BASE DE DATOS PARA REGISTRO DE ORDEN
    order_dataframe=pd.DataFrame(askOrderDetails[14]).head(1)
    append_order_dataframe=append_order_dataframe.append(order_dataframe,sort=False)

    #_____QUITAR COLUMNAS INNECESARIAS
    append_order_dataframe=append_order_dataframe[["ID","ACCOUNT_ID","AMOUNT","CREATED_AT","FEE_CURRENCY","LIMIT","MARKET_ID","ORIGINAL_AMOUNT","PAID_FEE","PRICE_TYPE","STATE","TOTAL_EXCHANGED","TRADED_AMOUNT","TYPE"]]

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

    #_____SUBIR FILAS FALTANTES A GOOGLE CLOUD
    bigquery_client.insert_rows(bigquery_client.get_table(table_ref), append_order_dataframe.values.tolist())

    #_____ACTUALIZAR VARIABLES
    askOrderDetails=None
    askOrderId=None

# CREAR ORDEN ASK (LIMIT)
def createAsk(limitAskPrice):
    
    global MONEY
    global CRYPT
    global client
    global areCRY
    global pastAsks
    global pastBids
    global askVolume
    global askOrderId
    global balanceCRY
    global gotAskOrder
    global owners_warning
    global marketDecimals
    global minVolumeTrade
    global askOrderDetails
    global theorySellPrice

    #_____TRATAR DE CANCELAR ORDEN ASK SI LLEGASE A EXITIR UNA
    while askOrderId!=None:
        try:
            cancelAsk()
        except:
            pass

    #_____ACTUALIZAR SALDO CRYPTO EN LA CUENTA
    amountCRY = getCRYinAccount()
    
    #_____SI CANTIDAD DE CRYPTO SUPERA EL MÍNIMO VOLUMEN DE TRANSACCIÓN
    if round_decimals_down(askVolume,marketDecimals) >= minVolumeTrade:
        
        #_____SI EL SALDO DE CRYPTO SUPERA EL VOLUMEN IDEAL DE TRANSACCIÓN
        if (amountCRY >= askVolume):
            areCRY = True
            amountAsk = askVolume

            #_____MONTAR ORDEN ASK (LIMIT)
            while True:
                try:
                    orden = client.new_order(CRYPT.lower()+"-"+MONEY.lower(), "ask", "limit", amountAsk, limitAskPrice)
                    askOrderId = orden.id
                    askOrderDetails = client.order_details(askOrderId)
                    theorySellPrice=limitAskPrice
                    break
                except:
                    print("[[ERROR]]: createAsk(limitAskPrice)")

            #_____TESTIGO DE CREACIÓN DE ORDEN
            gotAskOrder = True                    
        
        #_____SI NO TENGO LAS CRYPTOS SUFICIENTES
        else:
            print("[[ERROR]]: createAsk(limitAskPrice) -> warning: no tengo los recursos suficientes")
            subject = "SpreadNet: WARNING"
            msg = "[[ERROR]]: createAsk(limitAskPrice) -> warning: no tengo los recursos suficientes<br><br>My Money: <b>${}</b><br><br>My Crypt: <b>{}</b>".format(round(getMONinAccount(),2),round(amountCRY,4))
            owners = json.loads(config.get('owner_information','OWNERS_TRADE'))
            enviar_alerta(subject, msg, owners_warning)

# CANCEL BID ORDER + WRITE BIGQUERY DATABSE
def cancelBid():

    global CRYPT
    global MONEY
    global client
    global bidOrderId
    global gotBidOrder
    global theorySellPrice
    global bidOrderDetails
    global theorySellExecuted
    
    #_____ACTUALIZAR DETALLES DE LA ORDEN
    while True:
        try:
            bidOrderDetails = client.order_details(bidOrderId)
            break
        except:
            print("[[ERROR]]: cancelBid() -> bidOrderDetails = client.order_details(bidOrderId)")
    
    #_____SI LA ORDEN SE EJECUTÓ PARCIAL O TOTALMENTE
    if (bidOrderDetails.traded_amount.amount > 0.0):
        theorySellExecuted=theorySellPrice
        write_buy_sell_prices()
    
    #_____CANCELAR LA ORDEN A COMO DE LUGAR
    while True:
        try:
            client.cancel_order(bidOrderId)
            break
        except:
            print("[[ERROR]]: cancelBid() -> bidOrderDetails = client.cancel_order(bidOrderId)")
    
    #_____ACTUALIZAR DETALLES DE LA ORDEN
    while True:
        while True:
            try:
                bidOrderDetails = client.order_details(bidOrderId)
                break
            except:
                print("[[ERROR]]: cancelBid() -> bidOrderDetails = client.order_details(bidOrderId) (2)")

        #_____SI LA ORDEN YA SE MUESTRA COMO CANCELADA O TRANSADA
        if (bidOrderDetails.state=="canceled") or (bidOrderDetails.state=="traded"):
            break
        else:
            
            #_____CANCELAR LA ORDEN
            while True:
                try:
                    client.cancel_order(bidOrderId)
                    break
                except:
                    print("[[ERROR]]: cancelBid() -> bidOrderDetails = client.cancel_order(bidOrderId) (2)")
    
    #_____CREAR CONEXIÓN CON BASE DE DATOS EN BIGQUERY
    bigquery_client=bigquery.Client(project="dogwood-terra-308100")
    dataset_ref=bigquery_client.dataset("spreadNet")
    table_ref=dataset_ref.table(CRYPT+"_"+MONEY)
    table=dataset_ref.table(CRYPT+"_"+MONEY)
    table=bigquery.Table(table)

    #_____ASIGNAR COLUMNAS A BASE DE DATOS DE LA ORDEN CANCELADA
    columns=["ID","ACCOUNT_ID","AMOUNT","CREATED_AT","FEE_CURRENCY","LIMIT","MARKET_ID","ORIGINAL_AMOUNT","PAID_FEE","PRICE_TYPE","STATE","TOTAL_EXCHANGED","TRADED_AMOUNT","TYPE","JSON"]
    append_order_dataframe=pd.DataFrame(columns=columns)

    #_____CREAR BASE DE DATOS PARA REGISTRO DE ORDEN
    order_dataframe=pd.DataFrame(bidOrderDetails[14]).head(1)
    append_order_dataframe=append_order_dataframe.append(order_dataframe,sort=False)

    #_____QUITAR COLUMNAS INNECESARIAS
    append_order_dataframe=append_order_dataframe[["ID","ACCOUNT_ID","AMOUNT","CREATED_AT","FEE_CURRENCY","LIMIT","MARKET_ID","ORIGINAL_AMOUNT","PAID_FEE","PRICE_TYPE","STATE","TOTAL_EXCHANGED","TRADED_AMOUNT","TYPE"]]

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
    if append_order_dataframe.at[0,"TYPE"]=="Bid":
        append_order_dataframe.at[0,"MY_OPERATIONAL_UTILITY_FIAT"]=append_order_dataframe.at[0,"TOTAL_EXCHANGED"]
    else:
        append_order_dataframe.at[0,"MY_OPERATIONAL_UTILITY_FIAT"]=-append_order_dataframe.at[0,"TOTAL_EXCHANGED"]
    append_order_dataframe.at[0,"MY_OPERATIONAL_UTILITY_USD"]=append_order_dataframe.at[0,"MY_OPERATIONAL_UTILITY_FIAT"]/append_order_dataframe.at[0,"MY_TRM"]

    #_____SUBIR FILAS FALTANTES A GOOGLE CLOUD
    bigquery_client.insert_rows(bigquery_client.get_table(table_ref), append_order_dataframe.values.tolist())

    #_____ACTUALIZAR VARIABLES
    bidOrderDetails=None
    bidOrderId=None

# CREAR ORDEN BID (LIMIT)
def createBid(limitBidPrice):
    
    global MONEY
    global CRYPT
    global client
    global areCRY
    global pastBids
    global pastBids
    global bidVolume
    global bidOrderId
    global balanceCRY
    global gotBidOrder
    global owners_warning
    global marketDecimals
    global minVolumeTrade
    global bidOrderDetails
    global theorySellPrice

    #_____TRATAR DE CANCELAR ORDEN BID SI LLEGASE A EXITIR UNA
    while bidOrderId!=None:
        try:
            cancelBid()
        except:
            pass

    #_____ACTUALIZAR SALDO CRYPTO EN LA CUENTA
    amountCRY = getCRYinAccount()
    
    #_____SI CANTIDAD DE CRYPTO SUPERA EL MÍNIMO VOLUMEN DE TRANSACCIÓN
    if round_decimals_down(bidVolume,marketDecimals) >= minVolumeTrade:
        
        #_____SI EL SALDO DE CRYPTO SUPERA EL VOLUMEN IDEAL DE TRANSACCIÓN
        if (amountCRY >= bidVolume):
            areCRY = True
            amountBid = bidVolume

            #_____MONTAR ORDEN BID (LIMIT)
            while True:
                try:
                    orden = client.new_order(CRYPT.lower()+"-"+MONEY.lower(), "bid", "limit", amountBid, limitBidPrice)
                    bidOrderId = orden.id
                    bidOrderDetails = client.order_details(bidOrderId)
                    theorySellPrice=limitBidPrice
                    break
                except:
                    print("[[ERROR]]: createBid(limitBidPrice)")

            #_____TESTIGO DE CREACIÓN DE ORDEN
            gotBidOrder = True                    
        
        #_____SI NO TENGO LAS CRYPTOS SUFICIENTES
        else:
            print("[[ERROR]]: createBid(limitBidPrice) -> warning: no tengo los recursos suficientes")
            subject = "SpreadNet: WARNING"
            msg = "[[ERROR]]: createBid(limitBidPrice) -> warning: no tengo los recursos suficientes<br><br>My Money: <b>${}</b><br><br>My Crypt: <b>{}</b>".format(round(getMONinAccount(),2),round(amountCRY,4))
            owners = json.loads(config.get('owner_information','OWNERS_TRADE'))
            enviar_alerta(subject, msg, owners_warning)

