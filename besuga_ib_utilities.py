# Standard library imports
import sys
import builtins
import traceback
from datetime import datetime

# Third party imports
import ib_insync as ibsync
import tkinter as tk
import mysql.connector as sqlconn
import pandas as pd
import numpy as np

# Local application imports
import besuga_ib_config as cf

def error_handling (e, initial_text='Exception'):
    print("\n ", initial_text)
    print("ntype: ", type(e))  # the exception instance
    print("args: ", e.args)  # arguments stored in .args
    print("Exception: ", e)  # __str__ allows args to be printed directly
    print("Traceback \n")
    traceback.print_exc()


def save_to_excel(data_frame, out_path = 'C:/Users/xavie/Documents/TEST.xlsx', sheet_name='Sheet 1'):
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
        #error_handling(e)
        raise


#   Obre un formulari per fer inputs (camps de l'input com a paràmetre)
def makeform(root, fields):
    try:
        entries = []
        for field in fields:
            row = tk.Frame(root)
            lab = tk.Label(row, width=15, text=field[0], anchor='w')
            v = tk.StringVar(root, value=field[1])
            ent = tk.Entry(row, textvariable=v)
            row.pack(side=TOP, fill=X, padx=5, pady=5)
            lab.pack(side=LEFT)
            ent.pack(side=RIGHT, expand=YES, fill=X)
            entries.append((field, ent.get()))
            field[1] = ent.get()
        return fields
    except Exception as e:
        #error_handling(e)
        raise

    
# calcula dies distància entre dues dates
def diffdaysfromtoday(date1):
    try:
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
        return abs(delta.days)
    except Exception as e:
        #error_handling(e)
        raise


# calcula dies de distància des de datetoday
def diffdays(date1,date2):
    try:
        #entrem les dates en format 20181026, l'ordre de les datas és indiferent
        #calulem la distància en dies entre les dues dates en termes absoluts
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
        return abs(delta.days)
    except Exception as e:
        #error_handling(e)
        raise


def get_openpositions(ib):
    try:
        pfl=ib.portfolio()
        print(pfl)
        lst = []
        for i in range(len(pfl)):
            print(pfl[i])
            print(pfl[i].contract.secType)
            lst2  = []
            lst2.append(pfl[i].account)                                                                         #lst2[0]
            lst2.append(pfl[i].contract.conId)                                                                  #lst2[1]
            lst2.append(pfl[i].position)                                                                        #lst2[2]
            lst2.append(1)   # indicates Open position                                                          #lst2[3]
            lst2.append(pfl[i].marketPrice)                                                                     #lst2[4]
            lst2.append(pfl[i].marketValue)                                                                     #lst2[5]
            mult = pfl[i].contract.multiplier                                                                   #lst2[6]
            lst2.append(pfl[i].averageCost / float(mult)) if mult != '' else lst2.append(pfl[i].averageCost)
            lst2.append(pfl[i].averageCost)                                                                     #lst2[7]
            lst2.append(pfl[i].unrealizedPNL)                                                                   #lst2[8)
            lst2.append(pfl[i].realizedPNL)                                                                     #lst2[9]
            lst.append(lst2)
        return (lst)
    except Exception as err:
        #error_handling(err)
        raise


# Torna una llista amb un objectePNL [PnL(account='...', dailyPnL=nnnn, unrealizedPnL=-nnn, realizedPnL=nnn)]
def get_pnl(ib, accid):
    ib.reqPnL(accid, '')
    ib.sleep(8)
    pnl = ib.pnl()
    ib.cancelPnL(accid, '')
    return pnl


# Torna el PNLSingle per un contracte específic
# [PnLSingle(account='....', conId=nnn, dailyPnL=nnn, unrealizedPnL=nnn, realizedPnL=nnn, position=-n, value=nnn)]
def get_pnlsingle(ib, accid, conid):
    ib.reqPnLSingle(accid, '', conid)
    ib.sleep(8)
    pnl = ib.pnlSingle()
    ib.cancelPnLSingle(accid, '', conid)
    return pnl


def accountAnalysis(ib):
    try:
        accSum = ib.accountSummary()
        print(accSum)
        accountSummary = []
        for p in accSum:
            # print(p.tag, p.value)
            accountSummary.append((p.tag, p.value))
            dfaccountSummary = pd.DataFrame(accountSummary)
            print(dfaccountSummary)
    except Exception as err:
        #error_handling(err)
        raise


# Calcula el PNL  de les posicions obertes per tipus (calls, puts o stocks)
def dbget_pnlbyright(ib):
    try:
        pnl = [0,0,0,0,0,0] #pnl = [num calls, PNL calls, num puts, PNL puts, num stocks, PNL stocks]
        for pos in ib.portfolio():
            i = 6       # 6 perquè peti si alguna cosa falla (sortirà de rang)
            if(pos.contract.secType == "STK"): i = 4
            elif (pos.contract.secType == "OPT"):
                if (pos.contract.right == "C"): i=0
                elif (pos.contract.right == "P"): i=2
            pnl[i] += 1
            pnl[i+1] += (pos.unrealizedPNL)
        return pnl
    except Exception as err:
        # error_handling(err)
        raise

# Not used??
#def get_contractticker(ib, opt):
#    try:
#        details = ib.reqContractDetails(opt)
#        ticker = ib.reqMktData(opt, '', False, False)            # això torna un objecte Ticker
#        l = 0
#        while (ticker.lastGreeks == None) and l < 5:  #          mini-bucle per esperar que es rebin els Greeks
#            ticker = ib.reqMktData(opt, '', False, False)
#            ib.sleep(1)
#            l += 1
#        return ticker
#    except Exception as err:
#        # error_handling(err)
#        raise


def get_optionfromunderlying(cnt, optright, strike, expdate):
    try:
        # preparem el nou trade: definim i qualifiquem la nova opció
        option = ibsync.Contract()
        option.symbol = cnt.symbol
        option.strike = strike
        option.secType = "OPT"
        option.exchange = cf.myprefexchange
        option.currency = cnt.currency
        option.right = optright
        option.lastTradeDateOrContractMonth = expdate
        return option
    except Exception as err:
        # error_handling(err)
        raise


# Torna un objecte Ticker que conté, entre d'altres, els diferents Greeks (ask, bid, last, model
def get_greeks(ib, opt):
    ib.qualifyContracts(opt)
    ib.reqMarketDataType(4)
    if opt.secType != "OPT": return None
    optticker = ib.reqMktData(opt, '100,101,104,105,106', False, False)
    l = 0
    while (optticker.lastGreeks == None) and l < 10:  # mini-bucle per esperar que es rebin els Greeks
        optticker = ib.reqMktData(opt, '100,101,104,105,106', False, False)
        ib.sleep(1)
        l += 1
    #return getattr(optticker, type)
    return optticker


# trunquem els decimals del preu per què IB accepti el preu
def formatPrice(price, prec):
    try:
        precision = prec
        newPrice = np.round(price, precision)
        return newPrice
    except:
        #error_handling(err)
        raise


def dbconnect(hname, dbname, uname, pwd):
    try:
        cnx = sqlconn.connect(
            host = hname,
            database = dbname,
            user = uname,
            passwd = pwd
        )
        return cnx
    except sqlconn.Error as err:
        if err.errno == sqlconn.errorcode.ER_ACCESS_DENIED_ERROR:
            initialtext = "Something is wrong with your user name or password"
            error_handling(err, initialtext)
        elif err.errno == sqlconn.errorcode.ER_BAD_DB_ERROR:
            initialtext = "Database does not exist"
            error_handling(err, initialtext)
        raise


def dbdisconnect(connection):
    try:
        connection.close()
    except Exception as e:
        #error_handling(e)
        raise


def dbcommit(connection):
    try:
        connection.commit()
    except Exception as e:
        #error_handling(e)
        raise

def dbrollback (connection):
    try:
        connection.rollback()
    except Exception as e:
        #error_handling(e)
        raise

# Executa la query. colnames =  True torna una NamedTuple usant els noms de les columnes de la BD
def execute_query(db, query, values=None, commit=True, colnames = False):
    try:
        cursor = db.cursor()
        cursor.execute(query, values)
        if colnames:
            columns = cursor.description
            return [{columns[index][0]: column for index, column in enumerate(value)} for value in cursor.fetchall()]
        else:
            if (query.startswith('SELECT')):
                return cursor.fetchall()
            elif (query.startswith('INSERT')):
                if commit: db.commit()
                return cursor.lastrowid
            elif (query.startswith('UPDATE') or query.startswith('DELETE')):
                if commit: db.commit()
                return cursor.rowcount
    except Exception as err:
        #error_handling(err)
        if (db.is_connected()):
            db.rollback()
            cursor.close()
            db.close()
        raise
    finally:
        if (db.is_connected()): cursor.close()


if __name__ == '__main__':
    try:
        import besuga_ib_utilities as ibutil
        myib = ibsync.IB()
        mydb = ibutil.dbconnect("localhost", "besuga", "xarnaus", "Besuga8888")
        acc = input("triar entre 'besugapaper', 'xavpaper', 'mavpaper1', 'mavpaper2'")
        if acc == "besugapaper":
            rslt = execute_query(mydb,"SELECT connHost, connPort, connAccId FROM connections WHERE connName = 'besugapaper7498'")
        elif acc == "xavpaper":
            rslt = execute_query(mydb, "SELECT connHost, connPort, connAccId FROM connections WHERE connName = 'xavpaper7497'")
        elif acc == "mavpaper1":
            rslt = execute_query(mydb, "SELECT connHost, connPort, connAccId FROM connections WHERE connName = 'mavpaper1'")
        elif acc == "mavpaper2":
            rslt = execute_query(mydb, "SELECT connHost, connPort, connAccId FROM connections WHERE connName = 'mavpaper2'")
        else:
            sys.exit("Unknown account!!")
        myib.connect(rslt[0][0], rslt[0][1], 3)
        myaccId = rslt[0][2]

        #opt = ibsync.Stock(symbol="SHOP", exchange="SMART", currency="USD")
        opt = ibsync.Option(symbol="SHOP", exchange="SMART", currency="USD", lastTradeDateOrContractMonth  = "20190726", strike = "310", right = "P")
        #myib.qualifyContracts(opt)
        print(get_greeks(myib, opt, "lastGreeks"))


        ibutil.dbcommit(mydb)
        ibutil.dbdisconnect(mydb)
        myib.disconnect()
    except Exception as err:
        error_handling(err)
        raise