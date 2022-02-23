#_____LIBRERÍAS
import os
import ast
import time
import requests
import functions
import pandas as pd
from dotenv import load_dotenv
from google.cloud import storage

#_____INICIALIZAR
askVolume = 0.0
bidVolume = 0.0

askOrderId=""

gotAskOrder = False
gotBidOrder = False
limitAskPrice = 0.0
limitBidPrice = 0.0

sleepWait=0
sleepApis=0

client=""
MONEY="COP"
CRYPTO="BTC"
owners_warning=""
priceDistance=0.0
minVolumeTrade=0.0
marketDecimals=0.0
tradeProportion=0.5

theoryBuyPrice=0.0
theoryBuyExecuted=0.0

theorySellPrice=0.0
theorySellExecuted=0.0

utilityMarginThreshold=0.0

maxTradingVolumeProportion=0.01

#_____LOOP INFINITO
while True:

  #_____CANCELAR CUALQUIER TIPO DE ORDEN EXISTENTE + BALANCEO DE ASK BID + ACTUALIZAR FIAT + CRYPTO DISPONIBLES
  functions.finishThemAll()
  
  #_____SI SE ESTÁ EN ESCENARIO ASK
  if (askVolume>0.0) and (bidVolume==0.0):
    
    #_____ACTUALIZAR LIBRO DE ORDENES + DETERMIAR EL PRECIO TEORICO TRANSACCION
    functions.updateLimits()

    #_____DETERMINAR SI HAY SPREAD
    utilityMargin=functions.validMargin(limitAskPrice, theoryBuyExecuted)

    #_____SI HAY SPREAD SUFICIENTE
    if utilityMargin > utilityMarginThreshold:
      
      #_____MONTAR POSICION ASK (LIMIT)
      functions.crateAsk(limitAskPrice)

      #_____LOOP HASTE QUE EJECUTO ASK
      while gotAskOrder:
        
        #_____ACTUALIZAR ORDEN MONTADA
        while True:
          try:
              askOrderDetails = client.order_details(askOrderId)
              break
          except:
            time.sleep(sleepApis)

        #_____REVISAR SI SE EJECUTÓ ORDEN ASK PARCIAL O TOTALMENTE
        if (askOrderDetails.traded_amount.amount > 0.0):
          
          #_____ESCRIBIR PRECIOS EJECUTADOS + CANCELAR ORDEN + BALANCEAR
          functions.cancelAsk()
          functions.balancing_Ask_Bid()
          gotAskOrder = False
        
        #_____ELSE NO ME EJECUTARON ASK
        else:
        
          #_____ACTUALIZAR LIBRO DE ORDENES (PRECIOS + VOLUMENES PUNTA)
          functions.updatePriceVolume()

          #####

          #_____SI HUBO CAMBIOS DEL MERCADO: PRECIO PUNTA CAMBIA + REDUZCO SPREAD INNECESARIAMENTE + SE MONTARON CAMBIANDO LA PROPORCIÓN
          if (newAskPrice != theorySellPrice) or \
            ((theorySellPrice+priceDistance<=newPreLimitAskPrice) and (newAskPrice == theorySellPrice)) or \
              ((float(askVolume/(newLimitAskVolume))<tradeProportion) and (newAskPrice == theorySellPrice)):

            #_____CANCELAR ASK
            functions.cancelAsk()
            functions.balancing_Ask_Bid()
            gotAskOrder = False

            #_____SI SE ESTÁ EN ESCENARIO ASK
            if (askVolume>0.0) and (bidVolume==0.0):

              #_____ACTUALIZAR LIBRO DE ORDENES + DETERMIAR EL PRECIO TEORICO TRANSACCION
              functions.updateLimits()
              
              #_____MONTAR POSICION ASK (LIMIT)
              functions.crateAsk(limitAskPrice)

  #####

  #_____ELIF SI SE ESTÁ EN ESCENARIO BID
  if (askVolume==0.0) and (bidVolume>0.0):
    
    #_____ACTUALIZAR LIBRO DE ORDENES + DETERMIAR EL PRECIO TEORICO TRANSACCION
    functions.updateLimits()

    #_____DETERMINAR SI HAY SPREAD
    utilityMargin=functions.validMargin(theorySellExecuted, limitBidPrice)

    #_____SI HAY SPREAD SUFICIENTE
    if utilityMargin > utilityMarginThreshold:
      
      #_____MONTAR POSICION BID (LIMIT)
      functions.crateBid(limitBidPrice)

      #_____LOOP HASTE QUE EJECUTO BID
      while gotBidOrder:
        
        #_____ACTUALIZAR ORDEN MONTADA
        while True:
          try:
              bidOrderDetails = client.order_details(bidOrderId)
              break
          except:
            time.sleep(sleepApis)

        #_____REVISAR SI SE EJECUTÓ ORDEN BID PARCIAL O TOTALMENTE
        if (bidOrderDetails.traded_amount.amount > 0.0):
          
          #_____ESCRIBIR PRECIOS EJECUTADOS + CANCELAR ORDEN + BALANCEAR
          functions.cancelBid()
          functions.balancing_Ask_Bid()
          gotBidOrder = False
        
        #_____ELSE NO ME EJECUTARON BID
        else:
        
          #_____ACTUALIZAR LIBRO DE ORDENES (PRECIOS + VOLUMENES PUNTA)
          functions.updatePriceVolume()

          #####

          #_____SI HUBO CAMBIOS DEL MERCADO: PRECIO PUNTA CAMBIA + REDUZCO SPREAD INNECESARIAMENTE + SE MONTARON CAMBIANDO LA PROPORCIÓN
          if (newBidPrice != theoryBuyPrice) or \
            ((theoryBuyPrice-priceDistance>=newPreLimitBidPrice) and (newBidPrice == theoryBuyPrice)) or \
              ((float(bidVolume/(newLimitBidVolume))<tradeProportion) and (newBidPrice == theoryBuyPrice)):

            #_____CANCELAR BID
            functions.cancelBid()
            functions.balancing_Ask_Bid()
            gotBidOrder = False

            #_____SI SE ESTÁ EN ESCENARIO BID
            if (bidVolume==0.0) and (bidVolume>0.0):

              #_____ACTUALIZAR LIBRO DE ORDENES + DETERMIAR EL PRECIO TEORICO TRANSACCION
              functions.updateLimits()
              
              #_____MONTAR POSICION ASK (LIMIT)
              functions.crateBid(limitBidPrice)

  #####

  #_____ELIF SI SE ESTÁ EN ESCENARIO ASK + BID
  if (askVolume>0.0) and (bidVolume>0.0):

    #_____ACTUALIZAR LIBRO DE ORDENES + DETERMIAR EL PRECIO TEORICO TRANSACCION
    functions.updateLimits()

    #_____DETERMINAR SI HAY SPREAD
    utilityMargin=functions.validMargin(limitAskPrice, limitBidPrice)

    #_____SI HAY SPREAD SUFICIENTE
    if utilityMargin > utilityMarginThreshold:

        #_____MONTO POSICION LIMIT ASK
        functions.crateAsk(limitAskPrice)

        #_____COLOCO POSICION LIMIT BID
        functions.crateBid(limitBidPrice)

        #_____LOOP HASTE QUE EJECUTO ASK O BID
        while (gotAskOrder or gotBidOrder):

          #_____ACTUALIZAR ORDEN ASK MONTADA
          while True:
            try:
                askOrderDetails = client.order_details(askOrderId)
                break
            except:
              time.sleep(sleepApis)

          #_____ACTUALIZAR ORDEN BID MONTADA
          while True:
            try:
                bidOrderDetails = client.order_details(bidOrderId)
                break
            except:
              time.sleep(sleepApis)

          #####

          #_____(SI ME EJECUTARON ASK) O (SI ME EJECUTARON BID)
          if (askOrderDetails.traded_amount.amount > 0.0) or (bidOrderDetails.traded_amount.amount > 0.0):

            #_____ESCRIBIR PRECIOS EJECUTADOS + CANCELAR ORDEN (ASK)
            functions.cancelAsk()
            gotAskOrder = False

            #_____ESCRIBIR PRECIOS EJECUTADOS + CANCELAR ORDEN + BALANCEAR
            functions.cancelBid()
            gotBidOrder = False

            #_____BALANCEAR
            functions.balancing_Ask_Bid()

          #####

          #_____ELSE NO ME EJECUTARON ASK NI BID
          else:

            #_____ACTUALIZAR LIBRO DE ORDENES (PRECIOS + VOLUMENES PUNTA)
            functions.updatePriceVolume()

            #_____SI HUBO CAMBIOS DEL MERCADO: PRECIO PUNTA CAMBIA + REDUZCO SPREAD INNECESARIAMENTE + SE MONTARON CAMBIANDO LA PROPORCIÓN
            if((newAskPrice != theorySellPrice) or \
              ((theorySellPrice+priceDistance<=newPreLimitAskPrice) and (newAskPrice == theorySellPrice)) or \
                ((float(askVolume/(newLimitAskVolume))<tradeProportion) and (newAskPrice == theorySellPrice))) or \
                  ((newBidPrice != theoryBuyPrice) or \
                    ((theoryBuyPrice-priceDistance>=newPreLimitBidPrice) and (newBidPrice == theoryBuyPrice)) or \
                      ((float(bidVolume/(newLimitBidVolume))<tradeProportion) and (newBidPrice == theoryBuyPrice))):

              #_____ESCRIBIR PRECIOS EJECUTADOS + CANCELAR ORDEN (ASK)
              functions.cancelAsk()
              gotAskOrder = False

              #_____ESCRIBIR PRECIOS EJECUTADOS + CANCELAR ORDEN + BALANCEAR
              functions.cancelBid()
              gotBidOrder = False

              #_____BALANCEAR
              functions.balancing_Ask_Bid()

              #_____ELIF SI SE ESTÁ EN ESCENARIO ASK + BID
              if (askVolume>0.0) and (bidVolume>0.0):

                #_____ACTUALIZAR LIBRO DE ORDENES + DETERMIAR EL PRECIO TEORICO TRANSACCION
                functions.updateLimits()

                #_____DETERMINAR SI HAY SPREAD
                utilityMargin=functions.validMargin(limitAskPrice, limitBidPrice)

                #_____SI HAY SPREAD SUFICIENTE
                if utilityMargin > utilityMarginThreshold:

                    #_____MONTO POSICION LIMIT ASK
                    functions.crateAsk(limitAskPrice)

                    #_____COLOCO POSICION LIMIT BID
                    functions.crateBid(limitBidPrice)