

#importem llibreries
import numpy as np
from ib_insync import *
import openpyxl as op
import pandas as pd
import os

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
    print(fil[a])
    a = a + 1
    #print(p.account, p.tag, p.value, p.currency)
   # position.append(p)
    #position.append((p.account,p.contract,p.position,p.avgCost))
    fills.append(p)
dffills = pd.DataFrame(fills)
print(dffills)