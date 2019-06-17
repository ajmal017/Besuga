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


def error_handling (e, initial_text='Exception'):
    print("\n ", initial_text)
    print("ntype: ", type(e))  # the exception instance
    print("args: ", e.args)  # arguments stored in .args
    print("Exception: ", e)  # __str__ allows args to be printed directly
    print("Traceback \n")
    traceback.print_exc()


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


def execute_query(db, query, values=None, commit=True):
    try:
        cursor = db.cursor()
        cursor.execute(query, values)
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
