#_____TABLAS
# 1: SUMMARY TABLE QUE RECOPILA
# A) SALDOS DE CADA CRYPTO + FIAT
# B) PRECIO CRYPTO EN FIAT DIADA
# C) FECHA SIMULACIÓN
# D) SIMULACIÓN FIAT A USD + SIMULACIÓN CRYPTO A USD
# E) GUARDAR REGISTRO SEGREGADO POR DIADA
# F) UTILIDAD ACUMULADA POR MERCADO

# 2: TRADING BOOK AÑADIR
# A) UTILIDAD ACUMULADA (OPERACIONAL)


#_____FUNCIONES
# 0: ACTUALIZO BASE DE DATOS + BALANCEO + ACTUALIZAR FIAT + CRYPTO DISPONIBLES
# 1: ACTUALIZAR ORDENES EXISTENTES + CANCELO ALL SI TENGO ORDENES + BALANCEO
# 2: ACTUALIZAR VARIABLE GLOBAL DE PRECIOS PUNTA DEL LIBRO DE ORDENES
# 3: COLOCA POSICION ASK + PRECIO ASK + DETALLE TRANSACCION ASK + ID TRANSACCION + TESTIGO DE QUE TENGO ORDEN ASK
# 4: COLOCA POSICION BID + PRECIO BID + DETALLE TRANSACCION BID + ID TRANSACCION + TESTIGO DE QUE TENGO ORDEN BID
# 5: ACTUALIZAR DETALLE TRANSACCION ASK + TRUE SI EJECUTARON + FALSE SI NO EJECUTARON
# 6: ACTUALIZAR DETALLE TRANSACCION BID + TRUE SI EJECUTARON + FALSE SI NO EJECUTARON
# 7: CANCELO TRANSACCION ASK + SUBO DATOS DE TRANSACCION ASK A BIGQUERY
# 8: CANCELO TRANSACCION BID + SUBO DATOS DE TRANSACCION BID A BIGQUERY
# 9: ACTUALIZAR SALDO DE FIAT + SALDO DE CRYPTO

#_____VARIABLES
# A: VALOR QUE ME DICE CUANTO ME DEBO ALEJAR DEL SPREAD ($)
# B: VALOR DEL SPREAD MINIMO AL CUAL DEBERIA OPERAR
# C: DETALLE TRANSACCION ASK
# D: DETALLE TRANSACCION BID
# E: PRECIO ASK
# F: PRECIO BID

#_____LIBRERÍAS

import os
import ast
import requests
import pandas as pd
from dotenv import load_dotenv
from google.cloud import storage

#_____LOOP INFINITO
while(True):
    gotOrder = False
    #_____LOOP PRIMERA ITERACION
        #_____CANCELAR CUALQUIER TIPO DE ORDEN EXISTENTE # 1
        finishThemAll()
        #_____BALANCEO DE ASK BID + ACTUALIZAR FIAT + CRYPTO DISPONIBLES # 0
        balancing_Ask_Bid()

        #_____SI NO TENGO NINGUNA ORDEN MONTADA
        if (not gotOrder):
          balancing_Ask_Bid()

          #####
          #_____SI ESTOY EN ESCENARIO ASK
          if (askVolume>0.0):
            #_____LOOP HASTA QUE MONTE LA TRANSACCION
            while (not gotOrder):
              #_____ACTUALIZAR LIBRO DE ORDENES # 2
              Actualizar_libro_ordenes() #Función 1
              #_____DETERMIAR EL PRECIO TEORICO TRANSACCION # A
              determinar_precio_teorico() #Función 2
              #_____DETERMINAR SI HAY SPREAD # B
              validMargin()
              #_____SI TENGO SPREAD
              if validMargin() > min_margin:

                #_____COLOCO POSICION LIMIT ASK # 3
                createAsk(limitAskPrice) #Función 3
                gotOrder = True 
                  #_____LOOP HASTE QUE EJECUTO ASK
                while(gotOrder):
                    #####
                    if (askOrderId!=None):
                        while True:
                            try:
                                askOrderDetails = client.order_details(askOrderId)
                                break
                            except:
                                time.sleep(waitException)
                    else:
                        askOrderDetails = None
                    #####

                    #_____SI ME EJECUTARON ASK # 5
                    if (askOrderDetails != None):
                        gotNewAsk = (askOrderDetails.traded_amount.amount > 0.0)
                        if (gotNewAsk):
                            theorySellExecuted=theorySellPrice
                            write_buy_sell_prices()
                            cancelAsk()
                    else:
                        gotNewAsk=False
                    if gotNewAsk:

                      #_____CANCELAR + GUARDAR DATOS DE TRANSACCION ASK # 6
                      cancelAsk() #Función 7
                      #_____BALANCEO DE ASK BID + ACTUALIZAR FIAT + CRYPTO DISPONIBLES # 0
                      balancing_Ask_Bid() #Función 0
                    #####
                    else:
                    #_____ELSE NO ME EJECUTARON ASK # 5
                    
                      #_____ACTUALIZAR LIBRO DE ORDENES # 2

                      #####

                      #_____SI YA NO SOY LA ORDEN PUNTA ASK

                        #_____CANCELAR ASK # 6

                        #_____ACTUALIZAR LIBRO DE ORDENES # 2

                        #_____DETERMIAR EL PRECIO TEORICO TRANSACCION # A

                        #_____DETERMINAR SI HAY SPREAD # B

                        #_____SI TENGO SPREAD

                            #_____COLOCO POSICION LIMIT ASK # 3

                        #_____ELSE NO TENGO SPREAD

                          #_____SALIR DEL LOOP DE EJECUCION

                      #####

                      #_____ELIF SE ME MONTARON

                        #_____NO SOY LA MAYOR PROPORCION

                          #_____CANCELAR ASK # 6

                          #_____ACTUALIZAR LIBRO DE ORDENES # 2

                          #_____DETERMIAR EL PRECIO TEORICO TRANSACCION # A

                          #_____DETERMINAR SI HAY SPREAD # B

                          #_____SI TENGO SPREAD

                              #_____COLOCO POSICION LIMIT ASK # 3

                          #_____ELSE NO TENGO SPREAD

                            #_____SALIR DEL LOOP DE EJECUCION

                        #_____SI SIGO SIENDO LA MAYOR PROPORCION

                          #_____ESPERAR

                      #####

                      #_____ELIF SOY LA PUNTA PERO ESTOY CERRANDO INNECESARIAMENTE EL SPREAD

                        #_____CANCELAR ASK # 6

                        #_____ACTUALIZAR LIBRO DE ORDENES # 2

                        #_____DETERMIAR EL PRECIO TEORICO TRANSACCION # A

                        #_____DETERMINAR SI HAY SPREAD # B

                        #_____SI TENGO SPREAD

                            #_____COLOCO POSICION LIMIT ASK # 3

                        #_____ELSE NO TENGO SPREAD

                          #_____SALIR DEL LOOP DE EJECUCION

            #_____ELSE TENGO SPREAD

                #_____ESPERAR

    #####

    #_____ELIF Y SI ESTOY EN ESCENARIO BID

      #_____LOOP HASTA QUE MONTE LA TRANSACCION

        #_____ACTUALIZAR LIBRO DE ORDENES # 2

        #_____DETERMIAR EL PRECIO TEORICO TRANSACCION # A

        #_____DETERMINAR SI HAY SPREAD # B

        #_____SI TENGO SPREAD

            #_____COLOCO POSICION LIMIT BID # 4

            #_____LOOP HASTE QUE EJECUTO BID

              #####

              #_____SI ME EJECUTARON BID # 7

                #_____CANCELAR + GUARDAR DATOS DE TRANSACCION BID # 6

                #_____BALANCEO DE ASK BID + ACTUALIZAR FIAT + CRYPTO DISPONIBLES # 0

              #####

              #_____ELSE NO ME EJECUTARON BID # 7

                #_____ACTUALIZAR LIBRO DE ORDENES # 2

                #####

                #_____SI YA NO SOY LA ORDEN PUNTA BID

                  #_____CANCELAR BID # 11

                  #_____ACTUALIZAR LIBRO DE ORDENES # 2

                  #_____DETERMIAR EL PRECIO TEORICO TRANSACCION # A

                  #_____DETERMINAR SI HAY SPREAD # B

                  #_____SI TENGO SPREAD

                      #_____COLOCO POSICION LIMIT BID # 4

                  #_____ELSE NO TENGO SPREAD

                    #_____SALIR DEL LOOP DE EJECUCION

                #####

                #_____ELIF SE ME MONTARON

                  #_____NO SOY LA MAYOR PROPORCION

                    #_____CANCELAR BID # 11

                    #_____ACTUALIZAR LIBRO DE ORDENES # 2

                    #_____DETERMIAR EL PRECIO TEORICO TRANSACCION # A

                    #_____DETERMINAR SI HAY SPREAD # B

                    #_____SI TENGO SPREAD

                        #_____COLOCO POSICION LIMIT BID # 4

                    #_____ELSE NO TENGO SPREAD

                      #_____SALIR DEL LOOP DE EJECUCION

                  #_____SI SIGO SIENDO LA MAYOR PROPORCION

                    #_____ESPERAR

                #####

                #_____ELIF SOY LA PUNTA PERO ESTOY CERRANDO INNECESARIAMENTE EL SPREAD

                  #_____CANCELAR BID # 11

                  #_____ACTUALIZAR LIBRO DE ORDENES # 2

                  #_____DETERMIAR EL PRECIO TEORICO TRANSACCION # A

                  #_____DETERMINAR SI HAY SPREAD # B

                  #_____SI TENGO SPREAD

                      #_____COLOCO POSICION LIMIT BID # 4

                  #_____ELSE NO TENGO SPREAD

                    #_____SALIR DEL LOOP DE EJECUCION

        #_____SI NO TENGO SPREAD

          #_____ESPERAR

    #####

    #_____ELIF Y SI ESTOY EN ESCENARIO ASK + BID

      #_____LOOP HASTA QUE MONTE LA TRANSACCION

        #_____ACTUALIZAR LIBRO DE ORDENES

        #_____DETERMIAR EL PRECIO TEORICO TRANSACCION # A

        #_____DETERMINAR SI HAY SPREAD # B

        #_____SI TENGO SPREAD

            #_____COLOCO POSICION LIMIT ASK # 3

            #_____COLOCO POSICION LIMIT BID # 4

            #_____LOOP HASTE QUE EJECUTO ASK O BID

              #####

              #_____(SI ME EJECUTARON ASK # 5) O (SI ME EJECUTARON BID # 7)

                #_____(SI ME EJECUTARON ASK # 5) Y (SI ME EJECUTARON BID # 7)

                  #_____CANCELAR + GUARDAR DATOS DE TRANSACCION ASK # 6

                  #_____CANCELAR + GUARDAR DATOS DE TRANSACCION BID # 6

                  #_____SALIR DEL LOOP DE EJECUCION

                #_____ELIF (SI ME EJECUTARON ASK # 5) Y (NO ME EJECUTARON BID # 7)

                  #_____CANCELAR + GUARDAR DATOS DE TRANSACCION ASK # 6

                  #_____CANCELAR BID # 11

                  #_____SALIR DEL LOOP DE EJECUCION

                #_____ELIF (NO ME EJECUTARON ASK # 5) Y (SI ME EJECUTARON BID # 7)

                #_____CANCELAR + GUARDAR DATOS DE TRANSACCION BID # 6

                #_____CANCELAR ASK # 6

                #_____SALIR DEL LOOP DE EJECUCION

                #_____BALANCEO DE ASK BID + ACTUALIZAR FIAT + CRYPTO DISPONIBLES # 0

              #####

              #_____ELSE NO ME EJECUTARON ASK Y BID # 5

                #_____ACTUALIZAR LIBRO DE ORDENES # 2

                #####

                #_____(SI YA NO SOY LA ORDEN PUNTA ASK) O (SI YA NO SOY LA ORDEN PUNTA BID)

                  #_____CANCELAR BID # 11

                  #_____CANCELAR ASK # 6

                  #_____SALIR DEL LOOP DE EJECUCION

                #####

                #_____ELIF SE ME MONTARON EN ASK

                  #_____NO SOY LA MAYOR PROPORCION

                    #_____CANCELAR BID # 11

                    #_____CANCELAR ASK # 6

                    #_____SALIR DEL LOOP DE EJECUCION

                #_____ELIF SE ME MONTARON EN BID

                  #_____NO SOY LA MAYOR PROPORCION

                    #_____CANCELAR BID # 11

                    #_____CANCELAR ASK # 6

                    #_____SALIR DEL LOOP DE EJECUCION

                #####

                #_____ELIF SOY LA PUNTA PERO ESTOY CERRANDO INNECESARIAMENTE EL SPREAD ASK

                  #_____CANCELAR BID # 11

                  #_____CANCELAR ASK # 6

                  #_____SALIR DEL LOOP DE EJECUCION

                #_____ELIF SOY LA PUNTA PERO ESTOY CERRANDO INNECESARIAMENTE EL SPREAD BID

                  #_____CANCELAR BID # 11

                  #_____CANCELAR ASK # 6

                  #_____SALIR DEL LOOP DE EJECUCION

        #_____SI NO TENGO SPREAD

            #_____ESPERAR
