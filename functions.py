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

#_____FUNCIONES ESPECÍFICAS_____#

def crearDatabaseDesdeCero():
    
    global database_past_asks_bids
    global client
    global MONEY
    global CRYPT
    global wait
    global waitException
    
    database_past_asks_bids=pd.DataFrame()
    time.sleep(round(wait/10))
    pages=int(client.order_pages(market_id=CRYPT+"-"+MONEY)[1].total_pages)
    for page in range(1,pages+1):
        time.sleep(round(wait/10))
        data=pd.DataFrame(client.order_pages(market_id=CRYPT+"-"+MONEY,page=page)[0])
        database_past_asks_bids=database_past_asks_bids.append(data)
    database_past_asks_bids.reset_index(drop=True,inplace=True)
    database_past_asks_bids=database_past_asks_bids.loc[~database_past_asks_bids.limit.isna()]
    database_past_asks_bids.columns = database_past_asks_bids.columns.get_level_values(0)
    database_past_asks_bids.reset_index(drop=True,inplace=True)

    if len(database_past_asks_bids)>0:
        for i in range(0,len(database_past_asks_bids)):
            database_past_asks_bids.at[i,"amount_2"]=database_past_asks_bids.at[i,"amount"][0]
            database_past_asks_bids.at[i,"limit_2"]=database_past_asks_bids.at[i,"limit"][0]
            database_past_asks_bids.at[i,"original_amount_2"]=database_past_asks_bids.at[i,"original_amount"][0]
            database_past_asks_bids.at[i,"paid_fee_2"]=database_past_asks_bids.at[i,"paid_fee"][0]
            database_past_asks_bids.at[i,"total_exchanged_2"]=database_past_asks_bids.at[i,"total_exchanged"][0]
            database_past_asks_bids.at[i,"traded_amount_2"]=database_past_asks_bids.at[i,"traded_amount"][0]

        database_past_asks_bids.drop(["amount","limit","original_amount","paid_fee","total_exchanged","traded_amount"],axis=1,inplace=True)
        new_columns=['id','account_id','created_at','fee_currency','market_id','price_type','state','type','json','amount','limit','original_amount','paid_fee','total_exchanged','traded_amount']
        database_past_asks_bids.columns=new_columns
        database_past_asks_bids=database_past_asks_bids[["id","account_id","amount","created_at","fee_currency","limit","market_id","original_amount","paid_fee","price_type","state","total_exchanged","traded_amount","type","json"]]

    pickle_out = open("database_past_asks_bids_"+MONEY+"_"+CRYPT+".pickle","wb")
    pickle.dump(database_past_asks_bids, pickle_out)
    pickle_out.close()
