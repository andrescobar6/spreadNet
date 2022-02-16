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
    
    updatePast_Asks_Bids()
    
    if abs(pastAsks-pastBids)<minVolumeTrade:
        
        sellPrice=0.0
        buyPrice=0.0
        
        #______
    
        myActualMoney=getMONinAccount()
        myActualCrypt=getCRYinAccount()
        recomendedVolume=myActualCrypt*0.8

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

        if recomendedVolume>sellQuotation*0.8:
            recomendedVolume=sellQuotation*0.8
                
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
    
    volume=history_trades()
    controlVariable=abs(pastAsks-pastBids)  
    
    if abs(pastAsks-pastBids)<=minVolumeTrade:
        askVolume=volume
        bidVolume=volume
    elif pastAsks<pastBids:
        if (round_decimals_down(pastBids-pastAsks,5)>volume):
            subject = "SpreadNet: Stopped (Balancing Problem)"
            msg = "Revise SpreadNet: if (round_decimals_down(pastBids-pastAsks,5)>volume)"
            owners = json.loads(config.get('owner_information','OWNERS_ALERT'))
            enviar_alerta(subject, msg, owners)
        else:
            askVolume=round_decimals_down(pastBids-pastAsks,5) if (pastBids-pastAsks>=minVolumeTrade) else 0.0
            bidVolume=0.0
    elif pastAsks>pastBids:
        if (round_decimals_down(pastAsks-pastBids,5)>volume):
            subject = "SpreadNet: Stopped (Balancing Problem)"
            msg = "Revise SpreadNet: if (round_decimals_down(pastAsks-pastBids,5)>volume)"
            owners = json.loads(config.get('owner_information','OWNERS_ALERT'))
            enviar_alerta(subject, msg, owners)
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

    askPendings=0
    bidPendings=0

    askOrderId=None
    askOrderDetails=None

    bidOrderId=None
    bidOrderDetails=None
    
    Market = client_surbtc.getMarket(CRYPT+"-"+MONEY)
    data=pd.DataFrame(Market.getPendingOrders())
    
    if len(data)>0:
    
        askPendings=len(data.loc[((data.state=="pending") | (data.state=="accepted")) & (data.type=="Ask")])
        bidPendings=len(data.loc[((data.state=="pending") | (data.state=="accepted")) & (data.type=="Bid")])

        if (askPendings>0) or (bidPendings>0):

            askIdList=list(data.loc[((data.state=="pending") | (data.state=="accepted")) & (data.type=="Ask")].id.values)
            bidIdList=list(data.loc[((data.state=="pending") | (data.state=="accepted")) & (data.type=="Bid")].id.values)

            if len(askIdList)>0:

                for i in askIdList:
                    
                    while True:
                        try:
                            askOrderDetailsFinish = client.order_details(i)
                            break
                        except:
                            print("[ERROR]: finishThemAll() -> askOrderDetailsFinish = client.order_details(i)")
                            time.sleep(sleepError)

                    if (askOrderDetailsFinish.traded_amount.amount > 0.0):
                        testigoBigQuery=1
                        theorySellExecuted=theorySellPrice
                        write_buy_sell_prices()
                    
                    while True:
                        try:
                            client.cancel_order(i)
                            break
                        except:
                            print("[ERROR]: finishThemAll() -> client.cancel_order(i)")
                            time.sleep(sleepError)

                    while(True):

                        while True:
                            try:
                                askOrderDetailsFinish = client.order_details(i)
                                break
                            except:
                                print("[ERROR]: finishThemAll() -> askOrderDetailsFinish = client.order_details(i)(1)")
                                time.sleep(sleepError)

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

                    
                    if testigoBigQuery==1:

                        #_____CREAR CONEXIÓN CON BASE DE DATOS EN BIGQUERY
                        bigquery_client=bigquery.Client(project="dogwood-terra-308100")
                        dataset_ref=bigquery_client.dataset("spreadNet")
                        table_ref=dataset_ref.table(CRYPT+"_"+MONEY)
                        table=dataset_ref.table(CRYPT+"_"+MONEY)
                        table=bigquery.Table(table)

                        #_____SUBIR FILAS FALTANTES A GOOGLE CLOUD
                        rows_candle_list=list(np.arange(0,len(utilityTimeDataFrame_GoolgeCloud),10000))

                        for i in range(len(rows_candle_list)):
                            try:
                                bigquery_client.insert_rows(bigquery_client.get_table(table_ref), utilityTimeDataFrame_GoolgeCloud[rows_candle_list[i]:rows_candle_list[i+1]].values.tolist())
                            except:
                                bigquery_client.insert_rows(bigquery_client.get_table(table_ref), utilityTimeDataFrame_GoolgeCloud[rows_candle_list[i]:].values.tolist())




                        testigoBigQuery=0
                    
            if len(bidIdList)>=1:

                for j in bidIdList:

                    time.sleep(sleepApi)
                    while True:
                        try:
                            bidOrderDetailsFinish = client.order_details(j)
                            break
                        except:
                            time.sleep(sleepError)
                            #printOutput(f'client error sleepApiing {sleepError} seconds')
                    if (bidOrderDetailsFinish.traded_amount.amount > 0.0):
                        theoryBuyExecuted=theoryBuyPrice
                        last_volume_traded_fee=float(bidOrderDetailsFinish.paid_fee[0])+last_volume_traded_fee
                        createMarketBidFee()
                        write_buy_sell_prices()
                    time.sleep(sleepApi)
                    while True:
                        try:
                            client.cancel_order(j)
                            break
                        except:
                            time.sleep(sleepError)
                            #printOutput(f'client error sleepApiing {sleepError} seconds')
                    while(True):
                        time.sleep(sleepApi)
                        while True:
                            try:
                                bidOrderDetailsFinish = client.order_details(j)
                                break
                            except:
                                time.sleep(sleepError)
                                #printOutput(f'client error sleepApiing {sleepError} seconds')
                        if (bidOrderDetailsFinish.state=="canceled") or (bidOrderDetailsFinish.state=="traded"):
                            break
                        else:
                            time.sleep(sleepApi/2)
                            while True:
                                try:
                                    client.cancel_order(j)
                                    break
                                except:
                                    time.sleep(sleepError)
                                    #printOutput(f'client error sleepApiing {sleepError} seconds')

                    pickle_in = open("database_past_asks_bids_"+MONEY+"_"+CRYPT+".pickle","rb")
                    database_past_asks_bids= pickle.load(pickle_in)

                    columns=["id","account_id","amount","created_at","fee_currency","limit","market_id","original_amount","paid_fee","price_type","state","total_exchanged","traded_amount","type","json"]
                    appen_order_dataframe=pd.DataFrame(columns=columns)
                    order_dataframe=pd.DataFrame(bidOrderDetailsFinish[14]).head(1)
                    appen_order_dataframe=appen_order_dataframe.append(order_dataframe,sort=False)
                    database_past_asks_bids=database_past_asks_bids.append(appen_order_dataframe,sort=False)
                    database_past_asks_bids.drop_duplicates(subset=["id"],inplace=True)
                    database_past_asks_bids.reset_index(drop=True,inplace=True)

                    pickle_out = open("database_past_asks_bids_"+MONEY+"_"+CRYPT+".pickle","wb")
                    pickle.dump(database_past_asks_bids, pickle_out)
                    pickle_out.close()

        balancing_Ask_Bid()






