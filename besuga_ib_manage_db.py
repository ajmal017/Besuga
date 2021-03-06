# Standard library imports 
import sys
from datetime import datetime, timedelta

# Third party imports
import ib_insync as ibsync
import mysql.connector as sqlconn
from numpy import sign

# Local application imports
from besuga_ib_utilities import error_handling
from besuga_ib_utilities import execute_query
import besuga_ib_utilities as ibutil
import besuga_ib_config as cf


# Torna una llista d'objectes Contract que formen part d'alguna Execution
def get_currentcontracts(ib):
    try:
        execs = ib.reqExecutions()
        lst = []
        for i in range(len(execs)):
            contr = execs[i].contract
            ib.qualifyContracts(contr)
            print('Getting contract ' + str(contr.conId) + ' ' + str(contr.localSymbol))
            lst.append(contr)
        return(lst)
    except Exception as err:
        #error_handling(err)
        raise


# torna la data d'obertura d'una posició oberta. La torna en format YYYYmmdd (20120925)
# Si n'hi ha més d'una d'oberta pel mateix contracte, torna la més antiga
# ULL: xapussa a corretgir!!!!!!!!!!!!!!!!!!
def get_positiondate(db, accid, conid):
    try:
        sql = "SELECT DATE_FORMAT(pdate, '%Y%m%d') FROM positions " \
            " WHERE pAccId =  '" + str(accid) + "' AND pConId = " + str(conid) + " AND pActive = 1 " \
            " ORDER by pDate LIMIT 1"
        execs = execute_query(db, sql)
        if not execs:
            #raise Exception (" No hi ha cap posició a la base de dades pel contracte ", conid )
            print (" No hi ha cap posició a la base de dades pel contracte ", conid )
        elif len(execs)>1:
            raise Exception(" Hi ha més d'una posició oberta del contracte ", conid)
        else:
            return execs[0][0]
    except Exception as err:
        #error_handling(err)
        raise


# Mira si hi ha una posició oberta pel subjacent
def positionisopen(db, accid, symbol):
    sql = "SELECT * FROM openpositions WHERE pAccId =  '" + str(accid) + "' AND kSymbol = '" + str(symbol) + "' "
    execs = execute_query(db, sql)
    if execs != []:  return True
    else: return False


def inputearningsdate (conid , symbol ):
    try:
        q = input("Entra la Earnings Date (yyyymmdd) per " + str(conid) + "-" + str(symbol) + " - Return = '20880808' ")
        while q != "":
            if q.isdigit():
                if not (len(q) == 8 and int(q[:4]) > 2018 and int(q[4:6]) < 13 and int(q[6:8]) < 32):
                    q = input("Wrong format!! - Correct format is yyyymmdd ")
                else:
                    break
            else:
                q = input("Wrong format!! - Correct format is yyyymmdd ")
        if q == "": q = cf.mydefaultearndate        # Si apretem Enter, posem la per defect (20880808)
        return q
    except Exception as err:
        #error_handling(err)
        raise


# torna la 'earnings date' del contracte. La torna en format YYYYmmdd (20120925)
def getearningsdate(db, conid, symbol):
    try:
        sql = "SELECT DATE_FORMAT(kEarningsDate, '%Y%m%d') FROM contracts " \
              "WHERE kConId =  '" + str(conid) + "' "
        execs = execute_query(db, sql)
        if not execs:
            print(" El contracte  ", conid, " no existeix a la base de dades ")
            return None
        elif execs[0][0] == None:
            print(" La Earnings Date no existeix pel contracte ", str(conid), "-" , str(symbol))
            earningsdate = inputearningsdate(conid, symbol)
            # ULL!!!!!! Si la data no està entrada, posem un valor suficientment allunyat per no sortir de la posició
            if earningsdate != None:
                return datetime.strptime(earningsdate, "%Y%m%d").date()
            else:
                return (datetime.now() + timedelta(days=cf.mydaystoearnings + 1)).strftime("%Y%m%d")
        else:
            return datetime.strptime(execs[0][0], "%Y%m%d").date()
    except Exception as err:
        #error_handling(err)
        raise

def getprevioustargetprice(db, conid, accid ):
    try:
        sql = "SELECT cfs.fTargetPrice FROM contracts c RIGHT JOIN contractfundamentals cfs on c.kConId = cfs.fConId " + \
            " WHERE cfs.fConId = '" + str(conid) + "' AND cfs.fAccId = '" + str(accid) + "' AND cfs.fDate < DATE(NOW()) " + \
            " ORDER BY cfs.fDate DESC LIMIT 1 "
        rst = execute_query(db, sql)
        if rst != []: return rst[0][0]
        else: return 0
    except Exception as err:
        #error_handling(err)
        raise


def dbfill_earningsdate(db):
    try:
        # opció d'entrar-ne una o totes les buides
        q = input("Vols entrar totes les que estan buides o una d'específica? \n 'Press Enter' - Totes \n 'StockCode' - una de concreta \n\n")
        sql = "SELECT  kConId, kSymbol FROM contracts "
        if q != "":
            sql = sql + " WHERE kSymbol = '" + q + "' "
        else:
            sql = sql + " WHERE kEarningsDate IS NULL ORDER BY kSymbol "
        execs = execute_query(db, sql)
        if not execs:
            print(" No hi ha cap contracte que compleixi el criteri ")
        else:
            print("Aquesta és la llista:    ", execs)
            for i in range(len(execs)):
                earningsdate = inputearningsdate(execs[i][0], execs[i][1])
                sql = "UPDATE contracts SET kEarningsDate = " + str(earningsdate) + \
                      " WHERE kSymbol= '" + str(execs[i][1]) + "' "
                execute_query(db, sql)
    except Exception as err:
        # error_handling(err)
        raise


#Gets information form ib.Fills() and ib.Executions(). This information is only kept during 1 to 7 days in IB
def get_executions(ib):
    try:
        execs = ib.reqExecutions()
        lst = []
        for i in range(len(execs)):
            print('Getting execution ' + str(i) + ': ' + str(execs[i].execution.execId))
            lst2 = []   # els valors a inserir a la DDBB aniran a lst2 (1 lst2 per cada execs[i])
            lst2.append(execs[i].execution.execId)  # lst2[0]
            lst2.append(execs[i].execution.acctNumber)  # lst2[1]
            lst2.append(execs[i].execution.clientId)  # lst2[2]
            lst2.append(execs[i].execution.orderId)  # lst2[3]
            lst2.append(execs[i].contract.conId)  # lst2[4]

            lst2.append(execs[i].time)             #lst2[5]
            if (execs[i].execution.side == 'BOT'):  #lst2[6]
                lst2.append(execs[i].execution.shares)
            else:
                s = - execs[i].execution.shares
                lst2.append(s)
            lst2.append(execs[i].execution.price)   #lst2[7]
            lst2.append(execs[i].commissionReport.commission)   #lst2[8]
            if (execs[i].execution.liquidation is None):        #lst2[9]
                lst2.append(0)
            else:
                lst2.append(execs[i].execution.liquidation)
            # omplim els grecs, IV, etc si és una opció
            if execs[i].contract.secType == 'OPT':
                greeks = ibutil.get_greeks(ib, execs[i].contract).modelGreeks
                if (greeks is not None):
                    lst2.append(greeks.optPrice)           #lst2[10]
                    lst2.append(greeks.impliedVol)         #lst2[11]
                    lst2.append(greeks.delta)              #lst2[12]
                    lst2.append(greeks.gamma)              #lst2[13]
                    lst2.append(greeks.vega)               #lst2[14]
                    lst2.append(greeks.theta)              #lst2[15]
                    lst2.append(greeks.pvDividend)         #lst2[16]
                    lst2.append(greeks.undPrice)           #lst2[17]
                else:
                    lst2.extend([0]*8)
            else:
                # si no és una opció, ho deixem amb 0's
                lst2.extend([0] * 8)
            lst2.append(1)                                      # lst2[18]
            lst.append(lst2)                                    # lst2 (com a list) s'afegeix al final de la llista lst. Aquesta llista (lst) és la que retorna la funció
        return (lst)
    except Exception as err:
        #error_handling(err)
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
        #error_handling(err)
        raise


def dbanalyse_positions(db, accId):
    sql = "SELECT DISTINCT(ctConId) FROM combinedtrades WHERE ctAccId = '" + str(accId) + "' ORDER BY ctTime"
    try:
        lst = execute_query(db, sql)            # llista els diferents contractes a 'combinedtrades'
        final_list=[]
        for i in range(len(lst)):
            sql = "SELECT ctId, ctAccId, ctScanCode, ctTradeType, ctExecId, ctConId, ctType, ctMultiplier, ctShares, ctPrice, ctDate, ctCommission, ctLiquidation, ctoptPrice, ctoptIV, ctoptDelta, ctoptGamma, " \
                    + "ctoptTheta, ctoptVega, ctoptPVDividend, ctoptPriceOfUnderlying, ctActive FROM combinedtrades " \
                    + "WHERE ctAccId = '" + str(accId) + "' AND ctConId = " + str(lst[i][0]) + " ORDER BY ctActive, ctTime"
            execs = execute_query(db, sql)
            # mirem si l'últim registre està actiu (com a molt pot ser l'últim), si està actiu no cal fer-li res
            stop = len(execs)
            if execs[len(execs) - 1][21] == 1:
                stop = len(execs) - 1
            # mirem també si hi ha posicions comprades i venudes. Si totes soón del mateix signe no cal fer res
            list_numshares = [execs[item][8] for item in range(len(execs))]
            if  not(any(sign(item) != sign(list_numshares[0]) for item in list_numshares[1:])):
               stop = 0
            for h in range(0, len(execs)):  execs[h] = list(execs[h])  # convertim la tupla en una list
            j, new_k, new_j = 0, stop, stop
            for h in (y for y in range(j + 1, stop) if sign(execs[y][8]) != sign(execs[j][8])):
                new_k = h
                break
            k = min(new_k, stop)
            while j < stop:
                if abs(execs[j][8]) < abs(execs[k][8]):         # Comparació de les +/- shares
                    execs[j].append(execs[k])                   # append a la llista de j tota la llista de k com exec[j][20]
                    execs[k][8] = execs[j][8] + execs[k][8]     # recalculem el número de shares de k per la següent iteració
                elif abs(execs[j][8]) == abs(execs[k][8]):
                    execs[k][21] = 'D'                          # D for delete
                    execs[j].append(execs[k])                   # append a la llista de j tota la llista de k com exec[j][20]
                    # en aquest cas (k<j), tanquem execs[j]iinserim un nou element a la lliata amb la resta de shares de exec[j]
                    # ajustem la variable stop adequadament
                else:
                    execs[k][21] = 'D'                          # D for delete
                    aux = execs[j].copy()                       # aux és una llista auxiliar
                    aux[0] = execs[k][0]                        # utilitzem l'id de K (doncs sabem que execs[k] tanc auna posició, l'id no s'usarà
                    aux[6] = execs[j][8] + execs[k][8]          # el número de shares que quedaran al nou element
                    execs[j][8] = - execs[k][8]                 # ajustem el número de shares a execs[j] - posició que tanca
                    execs[j].append(execs[k])                   # append a la llista de j tota la llista de k com exec[j][20]
                    execs.insert(j+1,aux)                       # insertem el nou element a la posició j+1
                    stop += 1                                   # stop augmenta en un doncs afegim un element a la execs
                execs[j][21] = 'C'                                  # posarem el registre a tActive = (C)losed - SEMPRE
                new_j = stop
                for h in (x for x in range(j+1, stop) if execs[x][21] != 'D'):
                    new_j = h
                    for l in (y for y in range(new_j + 1, stop) if sign(execs[y][8]) != sign(execs[new_j][8])):
                        k = l
                        break
                    break
                j = min(new_j, stop)
            for h in range(0, len(execs)):
                final_list.append(execs[h])
        return final_list
    except Exception as err:
        #error_handling(err)
        raise


#   inserts contracts when contract  not in table mysql.contract
#   it can only be an 'INSERT' statement since a contract never changes
def dbfill_contracts(db, cntrlst):
    try:
        sql = "INSERT INTO contracts (kConId, kType, kSymbol, kLocalSymbol, kCurrency, kExchange, kTradingClass, kExpiry, kStrike, kRight, kMultiplier, kEarningsDate) "
        sql = sql + "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s ,%s, %s)"
        for i in range(len(cntrlst)):
            c = cntrlst[i]               # contract
            check = execute_query(db, "SELECT * FROM contracts WHERE kConId = " + str(c.conId))
            if (not check):
                earningsdate = inputearningsdate(c.conId, c.symbol)     # earnings date
                val = [c.conId, c.secType, c.symbol, c.localSymbol, c.currency, c.exchange, c.tradingClass, None, None, None, 1, earningsdate]
                # Si és una opció, reemplacem els valors específics d'opcions
                if (c.secType == 'OPT'):
                    val[-5:-1] = [c.lastTradeDateOrContractMonth, c.strike, c.right, c.multiplier]
                execute_query(db, sql, values = tuple(val), commit = True)
    except Exception as err:
        #error_handling(err)
        raise


#   inserta una order a la BD
def dbfill_orders(db, order, trade, scode, ttype):
    try:
        sql = "INSERT INTO orders (oOrderId, oClientId, oConId, oQuantity, oStatus, oScanCode, oTradeType) " \
              " VALUES (%s, %s, %s, %s, %s, %s, %s)"
        check = execute_query(db, "SELECT * FROM orders WHERE oOrderId = " + str(order.orderId) + " AND oClientId = " + str(order.clientId))
        if (not check):
            val = (order.orderId, order.clientId, trade.contract.conId, order.totalQuantity, trade.orderStatus.status, scode, ttype)
            execute_query(db, sql, val, True)
        else:
            print("Aquesta ordre ja existeix a la Base de Dades: ", order)
    except Exception as err:
        #error_handling(err)
        raise


def dbupdateone_contractfundamentals(db, accid, stk):
    try:
        cnt = stk[1]                # contract
        sql = "UPDATE contractfundamentals set  fEpsNext = %s, fFrac52wk = %s, fBeta = %s, fPE0 = %s, fDebtEquity = %s,  " \
                " fEVEbitda = %s, fPricetoFCFShare = %s, fYield = %s, fROE = %s, fTargetPrice = %s, fConsRecom = %s, fProjEPS = %s, fProjEPSQ = %s, fProjPE = %s " \
                " WHERE fConId = " + str(cnt.conId) + " AND fAccId = '" + str(accid) + "' "
        val = stk[2::]
        execute_query(db, sql, values = tuple(val), commit = True)
    except Exception as err:
        #error_handling(err)
        raise


# torna els últims fonamentals de la BD per un contracte determinat (o 0s si no existeix a la BD)
def dbget_lastfundamentals(db, conid):
    try:
        sql = "SELECT * FROM contractfundamentals WHERE fConId = " + str(conid) + " ORDER BY fDate DESC LIMIT 1"
        result = execute_query(db, sql)
        if  result == []: return [0]*17
        else: return list(result[0])
    except Exception as err:
        # error_handling(err)
        raise


def dbfill_fundamentals(db, accid, stklst):
    try:
        clst = [a[0] for a in stklst]
        dbfill_contracts(db, clst)
        for i in range(len(stklst)):
            c = stklst[i][0]
            check = execute_query(db, "SELECT * FROM contractfundamentals WHERE fConId = " + str(c.conId) +
                                  " AND fAccId = '" + str(accid) + "' AND fDate = DATE(NOW())")
            val = stklst[i][1:15]
            if (not check):
                datetoday = datetime.now().strftime("%Y%m%d")
                sql = "INSERT INTO contractfundamentals (fAccId, fConId, fDate, fEpsNext, fFrac52wk, fBeta, fPE0, " \
                      "fDebtEquity, fEVEbitda, fPricetoFCFShare, fYield, fROE, fTargetPrice, fConsRecom, fProjEPS, " \
                      "fProjEPSQ, fProjPE) " \
                      " VALUES ('" + str(accid) + "', '" + str(c.conId) + "', " + datetoday + ", %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            else:
                sql = "UPDATE contractfundamentals set fEpsNext = %s, fFrac52wk = %s, fBeta = %s, fPE0 = %s, fDebtEquity = %s, " \
                    " fEVEbitda = %s, fPricetoFCFShare = %s, fYield = %s, fROE = %s, fTargetPrice = %s, fConsRecom = %s, fProjEPS = %s, " \
                    " fProjEPSQ = %s, fProjPE = %s " \
                    " WHERE fConId = " + str(c.conId) + " AND fAccId = '" + str(accid) + "' AND fDate = DATE(NOW())"
            execute_query(db, sql, values = tuple(val), commit = True)
    except Exception as err:
        #error_handling(err)
        raise

# Inserta/actualitza els greeks per les posicions d'opcions obertes
def dbfillall_greeks(ib, db, accid):
    print("dbfillall_greeks")
    lst = execute_query(db, "SELECT DISTINCT(pConId) FROM positions WHERE pAccId = '" + str(accid) + "' AND pType = 'OPT' AND pActive = 1 ")
    for i in range(len(lst)):
        conid = lst[i][0]
        opt= ibsync.contract.Option(conId = str(conid))
        ib.qualifyContracts(opt)
        opttkr  = ibutil.get_greeks(ib, opt)
        lg = opttkr.lastGreeks
        if lg is not None:
            val = [lg.impliedVol, lg.optPrice, lg.delta, lg.gamma, lg.theta, lg.vega, lg.pvDividend, lg.undPrice]
        else: val = [0]*8
        mg = opttkr.modelGreeks
        if mg is not None:
            val.extend([mg.impliedVol, mg.optPrice, mg.delta, mg.gamma, mg.theta, mg.vega, mg.pvDividend, mg.undPrice])
        else: val.extend([0]*8)
        val.extend([opttkr.ask, opttkr.bid, opttkr.askSize, opttkr.bidSize])
        check = execute_query(db, "SELECT * FROM contractgreeks WHERE cgConId = " + str(conid) +
                              " AND cgAccId = '" + str(accid) + "' AND cgDate = DATE(NOW())")
        if (not check):
            datetoday = datetime.now().strftime("%Y%m%d")
            sql = "INSERT INTO contractgreeks (cgAccId, cgConId, cgDate, cglastIV, cglastOptPrice, cglastdelta, " \
                  "cglastgamma, cglasttheta, cglastvega, cglastDividend, cglastUndPrice, cgmodelIV, cgmodelOptPrice, " \
                  "cgmodeldelta, cgmodelgamma, cgmodeltheta, cgmodelvega, cgmodeldividend, cgmodelUndPrice, " \
                  "cgask, cgbid, cgasksize, cgbidsize) " \
                  " VALUES ('" + str(accid) + "', '" + str(conid) + "', " + datetoday + ", %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, " \
                                                                                        "%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
        else:
            sql = "UPDATE contractgreeks set cglastIV = %s, cglastOptPrice = %s, cglastdelta = %s, cglastgamma = %s, cglasttheta = %s, " \
                  " cglastvega = %s, cglastDividend = %s, cglastUndPrice = %s, cgmodelIV = %s, cgmodelOptPrice = %s, cgmodeldelta = %s, " \
                  " cgmodelgamma = %s, cgmodeltheta = %s, cgmodelvega = %s, cgmodeldividend = %s, cgmodelUndPrice = %s, " \
                  " cgask = %s, cgbid = %s, cgasksize = %s, cgbidsize = %s " \
                  " WHERE cgConId = " + str(conid) + " AND cgAccId = '" + str(accid) + "' AND cgDate = DATE(NOW())"
        execute_query(db, sql, values=tuple(val), commit=True)


#   inserts 'last' trades (IB only keeps Fills/Executions during 1 to 7 days)
#   IMPORTANT: queda pendent el tractament de les 'correccions' a IB: si n'hi ha una els darrers últims dígits de tExecId augmentaran en un (.01, .02...)
def dbfill_executions(db, execs):
    sql = "INSERT INTO trades (tExecid, tAccId, tClientId, tOrderId, tConId, tTime, tShares, tPrice, tCommission, tLiquidation, "
    sql = sql + "toptPrice, toptIV, toptDelta, toptGamma, toptVega, toptTheta, toptPVDividend, toptPriceOfUnderlying, tActive)"
    sql = sql + "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    for i in range(len(execs)):
        try:

            check = execute_query(db, "SELECT * FROM trades WHERE tExecId = '" + str(execs[i][0]) + "'")
            if (not check):
                print(i, " ", tuple(execs[i]))
                execute_query(db, sql, values=tuple(execs[i]), commit=True)
        except Exception as err:
            if err.errno == sqlconn.errorcode.ER_DUP_ENTRY:
                continue
            else:
                #error_handling(err)
                raise


def dbupdate_executions(db, execs):
    # execs[i] conté [tId, tExecId, tConId, tShares, tPrice, tActive]
    try:
        for i in range(len(execs)):
            print('Updating execution ' + str(execs[i][0]))
            if execs[i][5] == 0:
                sql = "UPDATE trades SET tActive = 0 where tId = " + str(execs[i][0])
                execute_query(db, sql, commit = True)
            elif execs[i][5] == 'M':
                sql = "UPDATE trades SET tShares = tShares - " + str(execs[i][3]) + " ,tActive = 0 where tId = " + str(execs[i][0])
                execute_query(db, sql, commit = True)
                sql = "INSERT INTO trades (tExecid, tAccId, tClientId, tOrderId, tConId, tTime, tShares, tPrice, tCommission, tLiquidation, "
                sql = sql + "toptPrice, toptIV, toptDelta, toptGamma, toptVega, toptTheta, toptPVDividend, toptPriceOfUnderlying, tActive) "
                # al nou registre, modifiquem l'Execid afegin-hi una C a davant, tActive=1 i tShares = execs[i][5]
                new_execid = 'C' + execs[i][1]
                sql = sql + "SELECT '" + new_execid + "',tAccId, tClientId, tOrderId, tConId, tTime," + str(execs[i][3]) + ", tPrice, tCommission, tLiquidation, "
                sql = sql + "toptPrice, toptIV, toptDelta, toptGamma, toptVega, toptTheta, "
                sql = sql + "toptPVDividend, toptPriceOfUnderlying, 1 "  # active = 1
                sql = sql + "FROM trades WHERE tId = " + str(execs[i][0])
                dbfill_positions (execute_query(db, sql, commit = True))
    except Exception as err:
        #error_handling(err)
        raise


def dbfill_positions(db, execs):
    # execs[i] conté [ctId, ctAccId, ctScanCode, ctTradeType, ctExecId, ctConId, ctType, ctMultiplier, ctShares, ctPrice, ctDate, ctCommission, ctLiquidation, ctoptPrice, ctoptIV, ctoptDelta, ctoptGamma,
    # ctoptTheta, ctoptVega, ctoptPVDividend, ctoptPriceOfUnderlying, ctActive
    # Si ctActive='C', llavors execs[i][19]=[execs[j]], on execs[j] és l'execució que tanca execs[i]
    try:
        # borrem la taula de positions (per aquest Account) i després farem inserts del què tenim a execs. La taula positions_optiondetails es borra també (DELETE CASCADE)
        if execs != []:
            sql = "DELETE FROM positions WHERE pAccId = '" + str(execs[0][1]) + "'"
            count = execute_query(db, sql, commit = True)
        for i in range(len(execs)):
            if execs[i][21] != 'D':
                print ('Inserting position ' + str(execs[i][0]))
                sql = "INSERT INTO positions (pId, pExecid, pAccId, pScanCode, pTradeType, pConId, pDate, pType, pMultiplier, pShares, pInitialPrice, pInitialValue, pCommission, pLiquidation, pActive) " \
                    + "SELECT ctId, ctExecId, ctAccId, ctScanCode, ctTradeType, ctConId, ctDate, ctType, ctMultiplier, ctShares, ctPrice, ctPrice*abs(ctShares)*ctMultiplier, ctCommission, ctLiquidation, ctActive " \
                    + "FROM combinedtrades WHERE ctID = " + str(execs[i][0])
                execute_query(db, sql, commit = True)
                if execs[i][6] == 'OPT':
                    sql = "INSERT INTO positions_optiondetails (podId, podInitialModelPrice, podInitialIV, podInitialDelta, podInitialGamma, podInitialVega, " \
                        + "podInitialTheta, podInitialPVDividend, podInitialPriceOfUnderlying) " \
                        + "SELECT ctId, ctoptPrice, ctoptIV, ctoptDelta, ctoptGamma, ctoptVega, ctoptTheta, ctoptPVDividend, ctoptPriceOfUnderlying " \
                        + "FROM combinedtrades WHERE ctID = " + str(execs[i][0])
                    execute_query(db, sql)
                if execs[i][21] == 'C':
                    clist = execs[i][22]
                    sql = "UPDATE positions set pActive = %s, pTradeType = %s, pClosingPrice = %s, pClosingValue = %s, pClosingDate = %s, pClosingId = %s, pPNL = %s, pCommission = %s, pLiquidation = %s " \
                        + "WHERE pId = " + str(execs[i][0])
                    pnl = -(execs[i][8]*execs[i][9] + clist[8]*clist[9])*execs[i][7]
                    val = (0, clist[3], clist[9], clist[7] * abs(clist[8]) * clist[9], clist[10], clist[0], pnl, execs[i][11] + clist[11], clist[12])
                    execute_query(db, sql, values = val, commit = True)
                    if execs[i][4] == 'OPT':
                        sql = "UPDATE positions_optiondetails set podFinalModelPrice = %s, podFinalIV = %s, podFinalDelta = %s, podFinalGamma = %s, podFinalTheta = %s,  " \
                            + "podFinalVega = %s, podFinalPVDividend = %s, podFinalPriceOfUnderlying = %s " \
                            + "WHERE podId = " + str(execs[i][0])
                        val = (clist[13], clist[14], clist[15], clist[16], clist[17], clist[18], clist[19], clist[20])
                        execute_query(db, sql, values = val, commit = True)
    except Exception as err:
        #error_handling(err)
        raise


def dbfill_accounthistory(ib, db, accid):
    print("Filling Account History ")
    try:
        dacs = ib.accountSummary()
        check = execute_query(db, "SELECT * FROM accounthistory WHERE achId = '" + str(accid) + "' AND achDate = DATE(NOW()) ")
        val= [accid]
        for i in range(1, 22): val.append(dacs[i].value)
        # pnl és una llista [PnL, num open Calls, open calls PNL, open Puts, open Puts PNL, ope Stocks, open Stocks PNL]
        realizedPNL = execute_query(db, "SELECT sum(pPNL) FROM positions WHERE pActive = 0")
        pnl = ibutil.get_pnl(ib, accid)
        val.extend([pnl[0].dailyPnL, pnl[0].unrealizedPnL + float(realizedPNL[0][0]), pnl[0].unrealizedPnL, float(realizedPNL[0][0])])
        val.extend(ibutil.dbget_pnlbyright(ib))
        val = tuple(val)

        if(not  check):
            sql = "INSERT INTO accounthistory (achId, achDate, achTime, achCushion, achDayTradesRemaining,achLookAheadNextChange,achAccruedCash,achAvailableFunds,achBuyingPower, " \
                  "achEquityWithLoanValue,achExcessLiquidity,achFullAvailableFunds,achFullExcessLiquidity,achFullInitMarginReq,achFullMaintMarginReq,achGrossPositionValue,achInitMarginReq, " \
                  "achLookAheadAvailableFunds,achLookAheadExcessLiquidity,achLookAheadInitMarginReq,achLookAheadMaintMarginReq,achMaintMarginReq,achNetLiquidation,achTotalCashValue, " \
                  "achDailyPNL, achTotalPNL, achUnrealizedPNL, achRealizedPNL, achOpenCalls, achOpenCallsPNL, achOpenPuts, achOpenPutsPNL, achOpenStocks, achOpenStocksPNL) " \
                  "VALUES (%s, DATE(NOW()), CURTIME(), %s,%s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s, %s, %s, %s, %s, %s, %s, %s, %s ) "
            execute_query(db, sql, values=val, commit=True)
        else:
            sql = "UPDATE accounthistory SET achTime = CURTIME(), achCushion = %s, achDayTradesRemaining = %s,achLookAheadNextChange = %s, achAccruedCash = %s,achAvailableFunds = %s,achBuyingPower = %s, " \
                  "achEquityWithLoanValue = %s,achExcessLiquidity = %s,achFullAvailableFunds = %s,achFullExcessLiquidity = %s,achFullInitMarginReq = %s,achFullMaintMarginReq = %s,achGrossPositionValue = %s,achInitMarginReq = %s, " \
                  "achLookAheadAvailableFunds = %s,achLookAheadExcessLiquidity = %s,achLookAheadInitMarginReq = %s,achLookAheadMaintMarginReq = %s,achMaintMarginReq = %s,achNetLiquidation = %s, achTotalCashValue = %s, " \
                  "achDailyPNL = %s, achTotalPNL = %s, achUnrealizedPNL = %s, achRealizedPNL = %s, achOpenCalls = %s, achOpenCallsPNL = %s, achOpenPuts = %s, achOpenPutsPNL = %s, achOpenStocks = %s, achOpenStocksPNL = %s " \
                  " WHERE achId = '" + str(accid) + "' AND achDate = DATE(NOW()) "
            val = val[1::]
            execute_query(db, sql, values = val, commit=True)
    except Exception as err:
        #error_handling(err)
        raise


def manage_positions(ib, db, accId):
    try:
        dbfill_contracts(db, get_currentcontracts(ib))                     # inserta tots els diferents contractes, si no hi són a la DB
        dbfill_executions(db, get_executions(ib))                   # inserta les noves (de 1 a 7 dies) executions
        dbupdate_executions(db, dbanalyse_executions(db,accId))   # actualitza les executions per tancar les que toqui
        dbfill_positions(db,dbanalyse_positions(db, accId))       # borra totes les positions i les re-inserta
    except Exception as err:
        #error_handling(err)
        raise


if __name__ == '__main__':
    try:
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

        #manage_positions(myib, mydb, myaccId)
        #get_currentcontracts(myib)
        #print(ibutil.get_openpositions(myib))
        #vow = ibsync.Stock(symbol = "BMY", exchange = "SMART", currency = "USD")
        #myib.qualifyContracts(vow)
        #dbget_lastfundamentals(mydb, vow.conId)
        #import besuga_ib_open_positions as ibopen
        #ibutil.tradelimitorder(myib, mydb, vow, 1, 2.7)
        #print(myib.reqAllOpenOrders())

        #dbfill_accounthistory(myib, mydb, myaccId)

        dbfillall_greeks(myib, mydb, myaccId)

        ibutil.dbcommit(mydb)
        ibutil.dbdisconnect(mydb)
        myib.disconnect()
    except Exception as err:
        error_handling(err)
        raise