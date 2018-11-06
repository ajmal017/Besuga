import tkinter as tk
from tkinter import messagebox as msgbox

import mysql.connector
import pandas as pd
from ib_insync import *
from mysql.connector import errorcode


#   funció per fer error handling
def error_handling (e, initial_text='Exception'):
    #mostra un pop-up d'error
    root = tk.Tk()
    root.withdraw()
    text=initial_text + 'Error Number({0}): {1}'.format(e.errno, e.strerror)
    msgbox.showerror("Error", text)


#   grava un DataFrame a excel
def save_to_excel(data_frame, out_path = 'C;/TEST.xlsx', sheet_name='Sheet 1'):
    # crea o sobreescriu un excel amb la informació del dataframe data_frame
    # out_path nome del fitxer (amb path: C:/xxx/yyy.xlsx)
    # sheeet_name nom del sheet
    try:
        writer = pd.ExcelWriter(out_path, engine='xlsxwriter')
        data_frame.to_excel(writer, sheet_name)
        writer.save()
    except IOError as e:
        error_handling(e,'I/O Error: ')
        raise
    except Exception as e:
        error_handling(e)
        raise


#   Obre un formulari per fer inputs (camps de l'input com a paràmetre)
def makeform(root, fields):
    entries = []
    print('fields1: 1: ', fields)
    for field in fields:
        row = Frame(root)
        lab = Label(row, width=15, text=field[0], anchor='w')
        v = StringVar(root, value=field[1])
        ent = Entry(row, textvariable=v)
        row.pack(side=TOP, fill=X, padx=5, pady=5)
        lab.pack(side=LEFT)
        ent.pack(side=RIGHT, expand=YES, fill=X)
        entries.append((field, ent.get()))
        print('field: ', field)
        field[1] = ent.get()
    print('fields: ', fields)
    return fields

    
# calcula dies distància entre dues dates
def diffdaysfromtoday(date1):
    #entrem la data en format 20181026
    #calculem la distància en dies respecte a avui de ña data emtrada em termes absoluts
    from datetime import date, datetime
    t1 = str(date.today())
    t2 = date1
    tf1= str(t1)[0:4] + ","+ str(t1)[5:7] + "," + str(t1)[8:10]
    tf2 = str(t2)[0:4] + "," + str(t2)[4:6] + "," + str(t2)[6:8]
    tf1 = datetime.strptime(tf1, "%Y,%m,%d")
    tf2 = datetime.strptime(tf2, "%Y,%m,%d")
    delta = tf2 - tf1
    print(abs(delta.days))
    return abs(delta.days)


# calcula dies de distància des de datetoday
def diffdays(date1,date2):
    #entrem les dates en format 20181026, l'ordre de les datas és indiferent
    #caluulem la distància en dies entre les dues dates en termes absoluts
    from datetime import datetime
    if date1 <= date2:
        t1 = date1
        t2 = date2
    else:
        t1 = date2
        t2 = date1
    tf1 = str(t1)[0:4] + "," + str(t1)[4:6] + "," + str(t1)[6:8]
    tf2 = str(t2)[0:4] + "," + str(t2)[4:6] + "," + str(t2)[6:8]
    tf1 = datetime.strptime(tf1, "%Y,%m,%d")
    tf2 = datetime.strptime(tf2, "%Y,%m,%d")
    delta = tf2 - tf1
    print(abs(delta.days))

def dbconnect():
    try:
        cnx = mysql.connector.connect(
            host='localhost',
            user='xarnaus',
            passwd='Besuga8888',
            database='besuga'
        )
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        print(err)
        raise
    return cnx


def dbdisconnect(connection):
    connection.disconnect


def execute_query(mydb, query, values=None):
    try:
        mycursor = mydb.cursor()
        mycursor.execute(query, values)
        if (query.startswith('SELECT')):
            return mycursor.fetchall()
        elif (query.startswith('INSERT')):
            mydb.commit()
            return mycursor.lastrowid
        elif (query.startswith('UPDATE') or query.startswith('DELETE')):
            mydb.commit()
            return mycursor.rowcount
    except Exception as err:
        raise

#   inserts contracts when contract not in table mysql.contract
#   it can only be an 'INSERT' statement since a contract never changes
def dbfill_contracts(db, contr):
    try:
        sql = "INSERT INTO contracts (kConId, kType, kSymbol, kLocalSymbol, kCurrency, kExchange, kTradingClass, kExpiry, kStrike, kRight, kMultiplier) "
        sql = sql + "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s ,%s)"
        for i in range(len(contr)):
            check = execute_query(mydb, "SELECT kConId FROM contracts WHERE kConId = " + str(contr[i][0]) )
            if (not check):
                execute_query(db, sql, tuple(contr[i]))
    except Exception as err:
        print(err)
        raise

#   inserts contracts when contract not in table mysql.contract
#   it can only be an 'INSERT' statement since a contract never changes
def dbfill_openpositions(db, pos):
    try:
        # fem un delete all + insert per mantenir la consistència
        for i in range(len(pos)):
            execute_query(db, "DELETE FROM openpositions")
            for i in range(len(pos)):
                sql = "INSERT INTO openpositions (pAccId, pConId, pPosition, pOpen, pMarketPrice, pMarketValue, pAverageCost, pUnrealizedPNL, pRealizedPNL) "
                sql = sql + "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s )"
                execute_query(db, sql, tuple(pos[i]))


        #for i in range(len(pos)):
        #    check = execute_query(mydb, "SELECT pNum FROM positions WHERE pAccId = '" + str(pos[i][0]) + "' AND pConId = " + str(pos[i][1]) + " AND pOpen = 1")
        #    if (not check):
        #        sql = "INSERT INTO positions (pAccId, pConId, pPosition, pOpen, pMarketPrice, pMarketValue, pAverageCost, pUnrealizedPNL, pRealizedPNL) "
        #        sql = sql + "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s )"
        #        execute_query(db, sql, tuple(pos[i]))
        #    else:
        #        sql = "UPDATE positions set pPosition = %s, pMarketPrice = %s, pMarketValue = %s, pAverageCost = %s, pUnrealizedPNL = %s, pRealizedPNL = %s "
        #        sql = sql  + "WHERE pNum = " + str(check[0][0])
        #        val = [pos[i][2], pos[i][4], pos[i][5], pos[i][6], pos[i][7], pos[i][8]]
        #        execute_query(db, sql, tuple(val))
    except Exception as err:
        print(err)
        raise


#   inserts daily trades (IB only keeps Fills/Executions during 1 day)
def dbfill_trades(db, trades):
    sql = "INSERT INTO trades (tExecid, tAccId, tConId, tTime, tShares, tPrice, tCommission, tLiquidation) "
    sql = sql + "VALUES (%s, %s, %s, %s, %s, %s, %s, %s)"
    for i in range(len(trades)):
        try:
            execute_query(db, sql, tuple(trades[i]))
        except Exception as err:
            if err.errno == errorcode.ER_DUP_ENTRY:
                continue
            else:
                print(err)
                raise


def get_contracts(ib):
    pfl=ib.portfolio()
    lst = []
    for i in range(len(pfl)):
        contr = pfl[i].contract
        ib.qualifyContracts(contr)
        lst2  = []
        lst2.append(contr.conId)        #lst2[0]
        lst2.append(contr.secType)      #lst2[1]
        lst2.append(contr.symbol)       #lst2[2]
        lst2.append(contr.localSymbol)  #lst2[3]
        lst2.append(contr.currency)     #lst2[4]
        lst2.append(contr.exchange)     #lst2[5]
        lst2.append(contr.tradingClass)
        if (contr.secType == 'OPT'):
            lst2.append(contr.lastTradeDateOrContractMonth)     #lst2[6]
            lst2.append(contr.strike)                           #lst2[7]
            lst2.append(contr.right)                            #lst2[8]
            lst2.append(contr.multiplier)                       #lst2[9]
        elif (contr.secType == 'STK'):
            lst2.extend([None, None, None, None])
        lst.append(lst2)
    return(lst)


def get_openpositions(ib):
    pfl=ib.portfolio()
    lst = []
    for i in range(len(pfl)):
        lst2  = []
        lst2.append(pfl[i].account)                     #lst2[0]
        lst2.append(pfl[i].contract.conId)              #lst2[1]
        lst2.append(pfl[i].position)                    #lst2[2]
        lst2.append(1)   # indicates Open position      #lst2[3]
        lst2.append(pfl[i].marketPrice)                 #lst2[4]
        lst2.append(pfl[i].marketValue)                 #lst2[5]
        lst2.append(pfl[i].averageCost)                 #lst2[6]
        lst2.append(pfl[i].unrealizedPNL)               #lst2[7]
        lst2.append(pfl[i].realizedPNL)                 #lst2[8]
        lst.append(lst2)
    return (lst)

#Gets information form ib.Fills() and ib.Executions(). This information is only kept for 1(?) day in IB
def get_trades(ib):
    fills = ib.reqExecutions()
    lst = []
    for i in range(len(fills)):
        lst2 = []
        lst2.append(fills[i].execution.execId)  # lst2[0]
        lst2.append(fills[i].execution.acctNumber)  # lst2[1]
        lst2.append(fills[i].contract.conId)  # lst2[2]
        lst2.append(fills[i].time)             #lst2[3]
        if (fills[i].execution.side == 'BOT'):  #lst2[4]
            lst2.append(fills[i].execution.shares)
        else:
            s = - fills[i].execution.shares
            lst2.append(s)
        lst2.append(fills[i].execution.price)   #lst2[5]
        lst2.append(fills[i].commissionReport.commission)   #lst2[6]
        if (fills[i].execution.liquidation is None):        #lst2[7]
            lst2.append(0)
        else:
            lst2.append(fills[i].execution.liquidation)
        lst.append(lst2)

        if fills[i].contract.secType == 'OPT':
            print (fills[i])
            ib.qualifyContracts(fills[i].contract)
            ib.reqMarketDataType(3)
            opt = ib.reqMktData(fills[i].contract, '100,101', False, False)
            ib.sleep(20)
            print("TEST: ", opt)

        #    reqId = ib.client.getReqId()
        #    print(reqId)
        #    ticker2 = ib.reqMktData(trade, "10,11,12,13,100,101",False, False)
        #    ib.sleep(11)
        #    print (trade)

    return (lst)


if __name__ == '__main__':

    myib = IB()
    mydb = dbconnect()
    #mycursor = mydb.cursor()

    rslt = execute_query(mydb,"SELECT connHost, connPort FROM connections WHERE connName = 'xavpaper7497'")
    myib.connect(rslt[0][0], rslt[0][1], 1)

    dbfill_contracts(mydb, get_contracts(myib))
    dbfill_openpositions(mydb, get_openpositions(myib))
    dbfill_trades(mydb, get_trades(myib))

    dbdisconnect(mydb)
    # desconnectem de IBAPI
    myib.disconnect()