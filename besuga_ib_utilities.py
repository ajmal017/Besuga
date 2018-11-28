import tkinter as tk
from tkinter import messagebox as msgbox

import mysql.connector
import pandas as pd
from numpy import sign
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


def execute_query(mydb, query, values=None, commit=True):
    try:
        mycursor = mydb.cursor()
        mycursor.execute(query, values)
        if (query.startswith('SELECT')):
            return mycursor.fetchall()
        elif (query.startswith('INSERT')):
            if commit: mydb.commit()
            return mycursor.lastrowid
        elif (query.startswith('UPDATE') or query.startswith('DELETE')):
            if commit: mydb.commit()
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
            execute_query(db, "DELETE FROM openpositions WHERE pOpen = 1")
            for i in range(len(pos)):
                sql = "INSERT INTO openpositions (pAccId, pConId, pPosition, pOpen, pMarketPrice, pMarketValue, pInitialPrice, pAverageCost, pUnrealizedPNL, pRealizedPNL) "
                sql = sql + "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s )"
                execute_query(db, sql, tuple(pos[i]))
    except Exception as err:
        print(err)
        raise


#   inserts daily trades (IB only keeps Fills/Executions during 1 day)
#   IMPORTANT: queda pendent el tractament de les 'correccions' a IB: si n'hi ha una els darrers últims dígits de tExecId augmentaran en un (.01, .02...)
def dbfill_executions(db, execs):
    sql = "INSERT INTO trades (tExecid, tAccId, tConId, tTime, tShares, tPrice, tCommission, tLiquidation, "
    sql = sql + "toptModelPrice, toptModelIV, toptModelDelta, toptModelGamma, toptModelVega, toptModelTheta, toptModelPVDividend, toptModelPriceOfUnderlying, tActive)"
    sql = sql + "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    for i in range(len(execs)):
        try:
            execute_query(db, sql, tuple(execs[i]))
        except Exception as err:
            if err.errno == errorcode.ER_DUP_ENTRY:
                continue
            else:
                print(err)
                raise


def dbget_combinedtrades(db):
    col = ['tExecId', 'tAccId', 'tConId', ]
    sql = "SELECT * FROM combinedtrades"
    try:
       table = pd.DataFrame(execute_query(db,sql))
       return(table)
    except Exception as err:
       print(err)
       raise


def dbanalyse_executions(db, accId):
    sql = "SELECT DISTINCT(tConId), min(tShares), max(tShares), COUNT(*) FROM activetrades WHERE tAccId = '" + str(accId) + "' " \
         + "GROUP BY tConId HAVING COUNT(tConId)>1 AND min(tShares) < 0 AND max(tShares)> 0 ORDER BY tConId, tTime"
    try:
        lst = execute_query(db, sql)            # distinct executions for the same contract + number of executions
        final_list=[]
        for i in range(len(lst)):
            j = k = last = stop =  0
            sql = "SELECT tId, tExecId, tConId, tShares, tPrice, tActive FROM activetrades " \
                  + "WHERE tAccId = '" + str(accId) + "' AND tConId = " + str(lst[i][0]) + " ORDER BY SIGN(tShares), tTime"
            execs = execute_query(db, sql)
            execs[0] = list(execs[0])            # convertim la tupla en una list
            # trobar l'índex(k)a partir del qual els valors són positius
            for h in range(1, len(execs)):
                execs[h] = list(execs[h])  # convertim la tupla en una list
                if (sign(execs[h][3]) != sign(execs[h-1][3])):
                    stop = h
                    k = h
            while j < stop:
                if k < len(execs):
                    if abs(execs[j][3]) < abs(execs[k][3]):        # Comparació de les +/- shares
                        execs[j][5] = 0                            # posarem el registre a tActive = 0
                        execs[k][3] = execs[j][3] + execs[k][3]
                        j += 1
                        last = k
                    elif abs(execs[j][3]) == abs(execs[k][3]):
                        execs[j][5] = 0
                        execs[k][5] = 0
                        last = len(execs)
                        j += 1
                        k += 1
                    else:
                        execs[k][5] = 0                              # posarem el registre a tActive = 0
                        execs[j][3] = execs[j][3] + execs[k][3]
                        last = j
                        k += 1
                else:
                    break
            if (last != len(execs)):                                # si last = len(execs), compres i vendes s'han quadrat
                execs[last][5] = 'M'                                # marca que s'ha canviat (ho posem a tActive)
            for j in range(0, len(execs)):
                final_list.append(execs[j])
        return final_list
    except Exception as err:
        print(err)
        raise


def dbanalyse_positions(db, accId):
    sql = "SELECT DISTINCT(ctConId) FROM combinedtrades WHERE ctAccId = '" + str(accId) + "' ORDER BY ctTime"
    try:
        lst = execute_query(db, sql)            # llista els diferents contractes a 'combinedtrades'
        final_list=[]
        for i in range(len(lst)):
            sql = "SELECT ctId, ctExecId, ctConId, ctType, ctMultiplier, ctShares, ctPrice, ctDate, ctCommission, ctLiquidation, ctoptModelPrice, ctoptModelIV, ctoptModelDelta, ctoptModelGamma, " \
                    + "ctoptModelTheta, ctoptModelVega, ctoptModelPVDividend, ctoptModelPriceOfUnderlying, ctActive FROM combinedtrades " \
                    + "WHERE ctAccId = '" + str(accId) + "' AND ctConId = " + str(lst[i][0]) + " ORDER BY ctActive, ctTime"
            execs = execute_query(db, sql)
            # mirem si l'últim registre està actiu (som a molt pot ser l'últim), si està actiu no cal fer-li res
            stop = len(execs)
            if execs[len(execs)-1][18] == 1:  stop = len(execs)-1
            for h in range(0, len(execs)):  execs[h] = list(execs[h])  # convertim la tupla en una list
            j = 0
            k = j+1
            while j < stop:
                while k < stop:
                    if abs(execs[j][5]) < abs(execs[k][5]):         # Comparació de les +/- shares
                        execs[j][18] = 'C'                           # posarem el registre a tActive = (C)losed
                        execs[j].append(execs[k])                     # append a la llista de j tota la llista de k com exec[j][6]
                        execs[k][5] = execs[j][5] + execs[k][5]     # recalculem el número de shares de k per la següent iteració
                    elif abs(execs[j][5]) == abs(execs[k][5]):
                        execs[j][18] = 'C'
                        execs[k][18] = 'D'                           # D for delete
                        execs[j].append(execs[k])                     # append a la llista de j tota la llista de k com exec[j][6]
                    else:
                        execs[k][18] = 'D'                            # D for delete
                        aux = execs[j]                                # Usem un auxiliar per la nova posició que caldrà crear i tancar contra exec[k]
                        aux[1] = 'C' + execs[j][1]
                        aux [5] = execs[k][5]
                        aux [18] = 'C'
                        execs[j].append(aux)
                        execs[j][5] = execs[j][5] + execs[k][5]      # recalculem el número de shares de j per la següent iteració
                    if (execs[k][18] == 'D'):
                        new_k = stop
                        for h in range (k+1, stop):
                            if sign(execs[h][5]) == sign(execs[k][5]):
                                new_k = h
                                break
                        k = new_k
                    if (execs[j][18] == 'C'): break
                new_j = stop
                for h in range(j + 1, stop):
                    if sign(execs[h][5]) == sign(execs[j][5]):
                        new_j = h
                        break
                j = new_j
            for h in range(0, len(execs)):
                final_list.append(execs[h])
        return final_list
    except Exception as err:
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
            lst2.extend([None, None, None, 1])                  # posem el multiplier a 1 per Stocks
        lst.append(lst2)
    return(lst)


def get_openpositions(ib):
    pfl=ib.portfolio()
    lst = []
    for i in range(len(pfl)):
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


#Gets information form ib.Fills() and ib.Executions(). This information is only kept for 1(?) day in IB
def get_executions(ib):
    execs = ib.reqExecutions()
    lst = []
    for i in range(len(execs)):
        lst2 = []   # els valors a inserir a la DDBB aniran a lst2 (1 lst2 per cada execs[i])
        lst2.append(execs[i].execution.execId)  # lst2[0]
        lst2.append(execs[i].execution.acctNumber)  # lst2[1]
        lst2.append(execs[i].contract.conId)  # lst2[2]
        lst2.append(execs[i].time)             #lst2[3]
        if (execs[i].execution.side == 'BOT'):  #lst2[4]
            lst2.append(execs[i].execution.shares)
        else:
            s = - execs[i].execution.shares
            lst2.append(s)
        lst2.append(execs[i].execution.price)   #lst2[5]
        lst2.append(execs[i].commissionReport.commission)   #lst2[6]
        if (execs[i].execution.liquidation is None):        #lst2[7]
            lst2.append(0)
        else:
            lst2.append(execs[i].execution.liquidation)
        # omplim els grecs, IV, etc si és una opció
        if execs[i].contract.secType == 'OPT':
            ib.qualifyContracts(execs[i].contract)
            ib.reqMarketDataType(4)
            opt = ib.reqMktData(execs[i].contract, '', False, False)
            while (opt.modelGreeks == None):  # mini-bucle per esperar que es rebin els Greeks
                ib.sleep(5)
                opt = ib.reqMktData(execs[i].contract, '', False, False)
            opt = ib.reqMktData(execs[i].contract, '100,101,105,106,107', False, False)
            lst2.append(opt.modelGreeks.optPrice)           #lst2[8]
            lst2.append(opt.modelGreeks.impliedVol)         #lst2[9]
            lst2.append(opt.modelGreeks.delta)              #lst2[10]
            lst2.append(opt.modelGreeks.gamma)              #lst2[11]
            lst2.append(opt.modelGreeks.vega)               #lst2[12]
            lst2.append(opt.modelGreeks.theta)              #lst2[13]
            lst2.append(opt.modelGreeks.pvDividend)         #lst2[14]
            lst2.append(opt.modelGreeks.undPrice)           #lst2[15]
        else:
            # si no és una opció, ho deixem amb 0's
            lst2.extend([0, 0, 0, 0, 0, 0, 0, 0])
        lst2.append(1)                                      # lst2[16]
        lst.append(lst2)                                    # lst2 (com a list) s'afegeix al final de la llista lst. Aquesta llista (lst) és la que retorna la funció
    return (lst)


def dbupdate_executions(db, execs):
    # execs[i] conté [tId, tExecId, tConId, tShares, tPrice, tActive]
    try:
        for i in range(len(execs)):
            if execs[i][5] == 0:
                sql = "UPDATE trades SET tActive = 0 where tId = " + str(execs[i][0])
                execute_query(db, sql)
            elif execs[i][5] == 'M':
                sql = "UPDATE trades SET tShares = tShares - " + str(execs[i][3]) + " ,tActive = 0 where tId = " + str(execs[i][0])
                execute_query(db, sql)
                sql = "INSERT INTO trades (tExecid, tAccId, tConId, tTime, tShares, tPrice, tCommission, tLiquidation, "
                sql = sql + "toptModelPrice, toptModelIV, toptModelDelta, toptModelGamma, toptModelVega, toptModelTheta, toptModelPVDividend, toptModelPriceOfUnderlying, tActive) "
                # al nou registre, modifiquem l'Execid afegin-hi una C a davant, tActive=1 i tShares = execs[i][5]
                new_execid = 'C' + execs[i][1]
                sql = sql + "SELECT '" + new_execid + "',tAccId, tConId, tTime," + str(execs[i][3]) + ", tPrice, tCommission, tLiquidation, "
                sql = sql + "toptModelPrice, toptModelIV, toptModelDelta, toptModelGamma, toptModelVega, toptModelTheta, "
                sql = sql + "toptModelPVDividend, toptModelPriceOfUnderlying, 1 "  # active = 1
                sql = sql + "FROM trades WHERE tId = " + str(execs[i][0])
                execute_query(db, sql)
    except Exception as err:
        print(err)
        raise


def dbupdate_positions(db, execs):
    # execs[i] conté [ctId, ctExecId, ctConId, ctType, ctMultiplier, ctShares, ctPrice, ctDate, ctCommission, ctLiquidation, ctoptModelPrice, ctoptModelIV, ctoptModelDelta, ctoptModelGamma,
    # ctoptModelTheta, ctoptModelVega, ctoptModelPVDividend, ctoptModelPriceOfUnderlying, ctActive
    # Si ctActive='C', llavors execs[i][19]=[execs[j]], on execs[j] és l'execució que tanca execs[i]
    try:
        for i in range(len(execs)):
            sql = "SELECT pActive from positions WHERE pId = " + str(execs[i][0])
            check = execute_query(db, sql)
            if check:
                if execs[i][18] == check[0][0]:
                    if execs[i][18] == 'C':
                        clist = execs[i][19]
                        sql = "UPDATE positions set pActive = %s, pClosingPrice = %s, pClosingValue = %s, pClosingDate = %s, pClosingId = %s, pPNL = %s, pCommission = %s, pLiquidation = %s " \
                            + "WHERE pId = " + str(execs[i][0])
                        pnl = -(execs[i][5]*execs[i][6] + clist[5]*clist[6])*execs[i][4]
                        values = (0, clist[6], clist[4]*abs(clist[5])*clist[6], clist[7], clist[0], pnl, execs[i][8]+clist[8], clist[9])
                        execute_query(db, sql, values)
                        if execs[i][3] == 'OPT':
                            sql = "UPDATE positions_optiondetails set podFinalModelPrice = %s, podFinalIV = %s, podFinalDelta = %s, podFinalGamma = %s, podFinalTheta = %s,  " \
                                + "podFinalVega = %s, podFinalPVDividend = %s, podFinalPriceOfUnderlying = %s " \
                                + "WHERE podId = " + str(execs[i][0])
                            values = (clist[10], clist[11], clist[12], clist[13], clist[14], clist[15], clist[16], clist[17])
                            execute_query(db, sql, values)
                    elif execs[i][18] == 'D':
                        sql = "DELETE FROM position_optiondetails where podId = " + str(execs[i][0])
                        execute_query(db, sql)
                        sql = "DELETE from positions where pid = " + str(execs[i][0])
            elif execs[i][18] != 'D':
                    sql = "INSERT INTO positions (pId, pExecid, pAccId, pConId, pDate, pType, pMultiplier, pShares, pInitialPrice, pInitialValue, pCommission, pLiquidation, pActive) " \
                        + "SELECT ctId, ctExecId, ctAccId, ctConId, ctDate, ctType, ctMultiplier, ctShares, ctPrice, ctPrice*abs(ctShares)*ctMultiplier, ctCommission, ctLiquidation, ctActive " \
                        + "FROM combinedtrades WHERE ctID = " + str(execs[i][0])
                    execute_query(db, sql)
                    if execs[i][3] == 'OPT':
                        sql = "INSERT INTO positions_optiondetails (podId, podInitialModelPrice, podInitialIV, podInitialDelta, podInitialGamma, podInitialVega, " \
                            + "podInitialTheta, podInitialPVDividend, podInitialPriceOfUnderlying) " \
                            + "SELECT ctId, ctoptModelPrice, ctoptModelIV, ctoptModelDelta, ctoptModelGamma, ctoptModelVega, ctoptModelTheta, ctoptModelPVDividend, ctoptModelPriceOfUnderlying " \
                            + "FROM combinedtrades WHERE ctID = " + str(execs[i][0])
                        execute_query(db, sql)
                    if execs[i][18] == 'C':
                        clist = execs[i][19]
                        sql = "UPDATE positions set pActive = %s, pClosingPrice = %s, pClosingValue = %s, pClosingDate = %s, pClosingId = %s, pPNL = %s, pCommission = %s, pLiquidation = %s " \
                            + "WHERE pId = " + str(execs[i][0])
                        pnl = -(execs[i][5]*execs[i][6] + clist[5]*clist[6])*execs[i][4]
                        values = (0, clist[6], clist[4] * abs(clist[5]) * clist[6], clist[7], clist[0], pnl, execs[i][8] + clist[8], clist[9])
                        execute_query(db, sql, values)
                        if execs[i][3] == 'OPT':
                            sql = "UPDATE positions_optiondetails set podFinalModelPrice = %s, podFinalIV = %s, podFinalDelta = %s, podFinalGamma = %s, podFinalTheta = %s,  " \
                                + "podFinalVega = %s, podFinalPVDividend = %s, podFinalPriceOfUnderlying = %s " \
                                + "WHERE podId = " + str(execs[i][0])
                            values = (clist[10], clist[11], clist[12], clist[13], clist[14], clist[15], clist[16], clist[17])
                            execute_query(db, sql, values)
    except Exception as err:
        print(err)
        raise


if __name__ == '__main__':

    myib = IB()
    mydb = dbconnect()

    rslt = execute_query(mydb,"SELECT connHost, connPort, connAccId FROM connections WHERE connName = 'xavpaper7497'")
    myib.connect(rslt[0][0], rslt[0][1], 1)
    myaccId = rslt[0][2]

    #dbfill_contracts(mydb, get_contracts(myib))
    #dbfill_openpositions(mydb, get_openpositions(myib))
    #dbfill_executions(mydb, get_executions(myib))


    aux = dbanalyse_executions(mydb,myaccId)
    print('AUX ' , aux)
    #dbupdate_executions(mydb, aux)

    aux2 = dbanalyse_positions(mydb, myaccId)
    dbupdate_positions(mydb,aux2)
    print('AUX2 ', aux2)

    dbdisconnect(mydb)
    # desconnectem de IBAPI
    myib.disconnect()