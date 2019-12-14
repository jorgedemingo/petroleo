# -*- coding: utf-8 -*-
"""
Created on Thu Dec 12 18:24:12 2019

@author: gfmar
"""
import sys
sys.path.append("..")

from ECFEPersonal import personal as p
from ECFEDiario import config as c
import pandas as pd
from ib_insync import IB, Future, util
import matplotlib.pyplot as plt
util.patchAsyncio()

futuro1 = 'CL'
exchange1 = 'NYMEX'
futuro2 = 'BZ'
exchange2 = 'NYMEX'
primerosN = 10
fDesde = '20191212'

def getClientId():
    usuario = p.usuario
    result = conexionBBDD.execute("select CLIENT_ID FROM CLIENT_ID WHERE USUARIO = %s", (usuario, ))
    fila = result.fetchone()
    clientId = int(fila.CLIENT_ID)
    clientId += 1
    if clientId == 100: clientId = 101
    if clientId == 1000: clientId = 1
    conexionBBDD.execute("UPDATE CLIENT_ID SET CLIENT_ID = %s WHERE USUARIO = %s", (clientId, usuario))
    return clientId

engine = c.engine
conexionBBDD = engine.connect()
clientID = getClientId()
conexionBBDD.close()

ib = IB()
ib.connect(host="127.0.0.1",port=7496, clientId=clientID)

'''FUTURO 1'''
'''Descargamos todos los datos del futuro continuo'''
details = ib.reqContractDetails(Future(futuro1, exchange1, includeExpired=True))
'''Cogemos solo los contratos, y filtramos los que tengan fecha superior a hoy'''

contracts = [d.contract for d in details if d.contract.lastTradeDateOrContractMonth > fDesde and d.contract.exchange == exchange1]
'''Necesitamos hacer esta chapu para poder ordenar y quedarnos con los N primeros'''
'''Creamos una lista de listas, que luego sí podemos ordenar'''
lista1 = []
for contract in contracts:
    lista2 = []
    lista2.append(contract.conId)
    lista2.append(contract.localSymbol)
    lista2.append(contract.lastTradeDateOrContractMonth)
    lista1.append(lista2)

'''Ordenamos la lista 1, con el segundo elemento de la sublista2'''
lista1.sort(key=lambda x: x[2])
'''Cogemos los N primeros'''
futuros1 = lista1[:primerosN]
'''Repetimos el mismo proceso para el futuro2'''
details = ib.reqContractDetails(Future(futuro2, exchange2, includeExpired=True))
contracts = [d.contract for d in details if d.contract.lastTradeDateOrContractMonth > fDesde and d.contract.exchange == exchange2]
lista1 = []
for contract in contracts:
    lista2 = []
    lista2.append(contract.conId)
    lista2.append(contract.localSymbol)
    lista2.append(contract.lastTradeDateOrContractMonth)
    lista1.append(lista2)

lista1.sort(key=lambda x: x[2])
futuros2 = lista1[:primerosN]

'''MEJORAR: Estoy dando por supuesto que los N primeros de las dos listas corresponden a los mismos meses'''
'''Esto no será siempre cierto, hay que mejorar este proceso para que tenga en cuenta los meses'''


df = pd.DataFrame(columns=('MES', 'CL', 'BZ', 'DIFF1', 'DIFF2','DIF'))
midF1ant = 0
midF2ant = 0
for i in range(0, len(futuros1)):
    contract = Future(localSymbol = futuros1[i][1], exchange= exchange1)
    ticker = ib.reqMktData(contract) 
    ib.sleep(2)                                                                                         
    ib.cancelMktData(contract)
    midF1 = round((ticker.bid + ticker.ask) / 2,3)
    if i == 0:
        difF1 = 0
    else:
        difF1 = midF1 - midF1ant
    midF1ant = midF1

    contract = Future(localSymbol = futuros2[i][1], exchange= exchange2)
    ticker = ib.reqMktData(contract) 
    ib.sleep(2)                                                                                         
    ib.cancelMktData(contract)
    midF2 = round((ticker.bid + ticker.ask) / 2,3)
    if i == 0:
        difF2 = 0
    else:
        difF2 = midF2 - midF2ant
    midF2ant = midF2
    
    dif = midF1 - midF2

    df.loc[i] = [futuros1[i][2][4:6], midF1, midF2, difF1, difF2, dif] 
print(df)    
df.plot()
    

ib.disconnect()