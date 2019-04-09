

#importem llibreries
import numpy as np
from ib_insync import *
import openpyxl as op
import pandas as pd
import os


def portfolio_to_dict(ib_):
    try:
        pfl = ib_.portfolio()
        # dictionary de lists que contindrà les dades que volem recuperar de l'objecte Contract per cada PortfolioItem del Portfolio
        d_contr = {'secType': [], 'conId': [], 'symbol': [], 'exchange': [], 'primaryExchange': [], 'currency': [], 'localSymbol': []}
        # OrderedDict de lists que contindrà les dades que volem recuperar de la namedtupla PortfolioItem (excepte els detalls del Contract) per cada PortfolioItem del Portfolio del Portfolio
        d_pfl = {'position': [], 'marketPrice': [], 'marketValue': [],'averageCost': [], 'unrealizedPNL': [], 'realizedPNL': [], 'account': []}
        # recorrem tots els PortfoioItema Portfolio
        for i in range(len(pfl)):
            ib_.qualifyContracts(pfl[i].contract)
            for k in d_contr.keys():
                # afegim els valors (cada value de (key,value) és una llista) de cada atribut que recuperem de l'objecte Contract d'aquest PortfolioItem.
                d_contr[k].append(getattr(pfl[i].contract, k))
            for k in d_pfl.keys():
                # afegim els valors (cada value de (key,value) és una llista) de cada valor que m'interessa de Portfolio Item ( a part dels detalls del contracte, recuperats abans)
                d_pfl[k].append(getattr(pfl[i], k))
        # posem tota la informació al dictionary pfl_values
        d_pfl.update(d_contr)
        # ordenem i retornem un OrderedDict
        my_order=['conId', 'symbol', 'localSymbol', 'currency', 'secType', 'position', 'averageCost', 'marketPrice', 'marketValue', 'unrealizedPNL', 'realizedPNL']
        od_pfl = OrderedDict((k, d_pfl[k]) for k in my_order)
        return od_pfl
    except Exception as e:
        msg='Exception in function portfolio_to_dict \n'
        error_handling(e, msg)
        raise

ib = IB()
ib.connect('127.0.0.1', 7496, clientId=1)


# Getting date today
print ("DATE TODAY\n")
import datetime
datetoday =datetime.datetime.now().strftime ("%Y%m%d")
print("datetoday    ",datetoday)

# IB.ACCOUNTSUMMARY
print("IB.ACCOUNTSUMMARY\n")
acsum =ib.accountSummary()

print("account summary   ",acsum)
accountSummary = []
a = 0
for p in acsum:
     print(acsum[a])
     a = a+1
     print(p.account, p.tag, p.value, p.currency)
     accountSummary.append((p.tag,p.value,p.currency))
dfaccountSummary = pd.DataFrame(accountSummary)
print(dfaccountSummary)

# ib.accountSummary és una llista de 2 nivells
print (acsum[4][1])


# IB.POSITIONS
print ("IB.POSITIONS\n")

pos = ib.positions()
print ("positons   ",pos)
position =[]
a=0
for p in pos:
    print(pos[a])
    a = a + 1
    #print(p.account, p.tag, p.value, p.currency)
   # position.append(p)
    #position.append((p.account,p.contract,p.position,p.avgCost))
    position.append(p)
dfPoaition = pd.DataFrame(position)
print(dfPoaition)


#IB.PORTFOLIO
print("IB.PORTFOLIO\n")

pfl = ib.portfolio()
print("portfolio     ", pfl)

port =[]
for p in pfl:
    #print(p.contract)
    #print(p[0],"   ",p[1],"   ",p[2],"   ",p[3],"   ",p[4],"   ",p[5],"   ",p[6])
    symbol = p.contract.symbol
    expirationdate = p.contract.lastTradeDateOrContractMonth
    print(symbol,"   ",expirationdate)
    port.append(p)
dfPort = pd.DataFrame(port)
print(dfPort)


#IB CONTRACT
con = Contract()
print ("IB.CONTRACT\n")
print (con)

#IB EXECUTIONS
print ("IB.EXECUTIONS\n")
exe = ib.executions()
print("executions     ",exe)
execution =[]
a=0
for p in exe:
    print(exe[a])
    a = a + 1
    #print(p.account, p.tag, p.value, p.currency)
   # position.append(p)
    #position.append((p.account,p.contract,p.position,p.avgCost))
    execution.append(p)
dfexecution = pd.DataFrame(execution)
print(dfexecution)

#IB FILLS
print ("IB.FILLS\n")
fil = ib.fills()
print("fills     ",fil)
fills =[]
a=0
for p in fil:
    subfills =[]
    time =""

    #print(fil[a])
    a = a + 1
    #print(p.account, p.tag, p.value, p.currency)
   # position.append(p)
    #position.append((p.account,p.contract,p.position,p.avgCost))
    #fills.append(p)
    subfills.append(p.contract.secType)
    subfills.append(p.contract.conId)
    subfills.append(p.contract.symbol)
    subfills.append(p.contract.strike)
    subfills.append(p.contract.right)
    subfills.append(p.contract.multiplier)
    subfills.append(p.contract.currency)


    subfills.append(p.execution.acctNumber)
    subfills.append(p.execution.side)
    subfills.append(p.execution.shares)
    subfills.append(p.execution.price)
    subfills.append(p.execution.cumQty)
    subfills.append(p.execution.avgPrice)
    time = str(p.time)[0:4]+str(p.time)[5:7]+str(p.time)[8:10]

    subfills.append(time)

    fills.append(subfills)
    print (subfills)
dffills = pd.DataFrame(fills)

print(dffills)
#print(fills)

