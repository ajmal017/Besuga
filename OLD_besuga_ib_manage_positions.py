from besuga_ib_utilities import *
from ib_insync import *
from numpy import sign
import sys


def get_contracts(ib):
    execs = ib.reqExecutions()
    lst = []
    for i in range(len(execs)):
        contr = execs[i].contract
        ib.qualifyContracts(contr)
        print('Getting contract ' + str(contr.conId) + ' ' + str(contr.localSymbol))
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
        else:
            lst2.extend([None, None, None, 1])                  # posem el multiplier a 1 per la resta d'instruments
        lst.append(lst2)
    return(lst)


#Gets information form ib.Fills() and ib.Executions(). This information is only kept during 1 to 7 days in IB
def get_executions(ib):
    execs = ib.reqExecutions()
    lst = []
    for i in range(len(execs)):
        print('Getting execution ' + str(execs[i].execution.execId))
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
            l = 0
            while (opt.lastGreeks == None) and l < 5:  # mini-bucle per esperar que es rebin els Greeks
                opt = ib.reqMktData(execs[i].contract, '', False, False)
                ib.sleep(5)
                l += 1
            opt = ib.reqMktData(execs[i].contract, '100,101,105,106,107', False, False)
            if (opt.lastGreeks is not None):
                lst2.append(opt.lastGreeks.optPrice)           #lst2[8]
                lst2.append(opt.lastGreeks.impliedVol)         #lst2[9]
                lst2.append(opt.lastGreeks.delta)              #lst2[10]
                lst2.append(opt.lastGreeks.gamma)              #lst2[11]
                lst2.append(opt.lastGreeks.vega)               #lst2[12]
                lst2.append(opt.lastGreeks.theta)              #lst2[13]
                lst2.append(opt.lastGreeks.pvDividend)         #lst2[14]
                lst2.append(opt.lastGreeks.undPrice)           #lst2[15]
            else:
                lst2.extend([0, 0, 0, 0, 0, 0, 0, 0])
        else:
            # si no és una opció, ho deixem amb 0's
            lst2.extend([0, 0, 0, 0, 0, 0, 0, 0])
        lst2.append(1)                                      # lst2[16]
        lst.append(lst2)                                    # lst2 (com a list) s'afegeix al final de la llista lst. Aquesta llista (lst) és la que retorna la funció
    return (lst)


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
            sql = "SELECT ctId, ctAccId, ctExecId, ctConId, ctType, ctMultiplier, ctShares, ctPrice, ctDate, ctCommission, ctLiquidation, ctoptPrice, ctoptIV, ctoptDelta, ctoptGamma, " \
                    + "ctoptTheta, ctoptVega, ctoptPVDividend, ctoptPriceOfUnderlying, ctActive FROM combinedtrades " \
                    + "WHERE ctAccId = '" + str(accId) + "' AND ctConId = " + str(lst[i][0]) + " ORDER BY ctActive, ctTime"
            execs = execute_query(db, sql)
            # mirem si l'últim registre està actiu (com a molt pot ser l'últim), si està actiu no cal fer-li res
            stop = len(execs)
            if execs[len(execs)-1][19] == 1:  stop = len(execs)-1
            for h in range(0, len(execs)):  execs[h] = list(execs[h])  # convertim la tupla en una list
            j, new_k, new_j = 0, stop, stop
            for h in (y for y in range(j + 1, stop) if sign(execs[y][6]) != sign(execs[j][6])):
                new_k = h
                break
            k = min(new_k, stop)
            while j < stop:
                if abs(execs[j][6]) < abs(execs[k][6]):         # Comparació de les +/- shares
                    execs[j].append(execs[k])                   # append a la llista de j tota la llista de k com exec[j][20]
                    execs[k][6] = execs[j][6] + execs[k][6]     # recalculem el número de shares de k per la següent iteració
                elif abs(execs[j][6]) == abs(execs[k][6]):
                    execs[k][19] = 'D'                          # D for delete
                    execs[j].append(execs[k])                   # append a la llista de j tota la llista de k com exec[j][20]
                    # en aquest cas (k<j), tanquem execs[j]iinserim un nou element a la lliata amb la resta de shares de exec[j]
                    # ajustem la variable stop adequadament
                else:
                    execs[k][19] = 'D'                          # D for delete
                    aux = execs[j].copy()                       # aux és una llista auxiliar
                    aux[0] = execs[k][0]                        # utilitzem l'id de K (doncs sabem que execs[k] tanc auna posició, l'id no s'usarà
                    aux[6] = execs[j][6] + execs[k][6]          # el número de shares que quedaran al nou element
                    execs[j][6] = - execs[k][6]                 # ajustem el número de shares a execs[j] - posició que tanca
                    execs[j].append(execs[k])                   # append a la llista de j tota la llista de k com exec[j][20]
                    execs.insert(j+1,aux)                       # insertem el nou element a la posició j+1
                    stop += 1                                   # stop augmenta en un doncs afegim un element a la execs
                execs[j][19] = 'C'                                  # posarem el registre a tActive = (C)losed - SEMPRE
                new_j = stop
                for h in (x for x in range(j+1, stop) if execs[x][19] != 'D'):
                    new_j = h
                    for l in (y for y in range(new_j + 1, stop) if sign(execs[y][6]) != sign(execs[new_j][6])):
                        k = l
                        break
                    break
                j = min(new_j, stop)
            for h in range(0, len(execs)):
                final_list.append(execs[h])
        return final_list
    except Exception as err:
        print(err)
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
                execute_query(db, sql, values = tuple(contr[i]))
    except Exception as err:
        print(err)
        raise


#   inserts 'last' trades (IB only keeps Fills/Executions during 1 to 7 days)
#   IMPORTANT: queda pendent el tractament de les 'correccions' a IB: si n'hi ha una els darrers últims dígits de tExecId augmentaran en un (.01, .02...)
def dbfill_executions(db, execs):
    sql = "INSERT INTO trades (tExecid, tAccId, tConId, tTime, tShares, tPrice, tCommission, tLiquidation, "
    sql = sql + "toptPrice, toptIV, toptDelta, toptGamma, toptVega, toptTheta, toptPVDividend, toptPriceOfUnderlying, tActive)"
    sql = sql + "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    for i in range(len(execs)):
        try:
            execute_query(db, sql, values = tuple(execs[i]))
        except Exception as err:
            if err.errno == errorcode.ER_DUP_ENTRY:
                continue
            else:
                print(err)
                raise


def dbupdate_executions(db, execs):
    # execs[i] conté [tId, tExecId, tConId, tShares, tPrice, tActive]
    try:
        for i in range(len(execs)):
            print('Updating execution ' + str(execs[i][0]))
            if execs[i][5] == 0:
                sql = "UPDATE trades SET tActive = 0 where tId = " + str(execs[i][0])
                execute_query(db, sql)
            elif execs[i][5] == 'M':
                sql = "UPDATE trades SET tShares = tShares - " + str(execs[i][3]) + " ,tActive = 0 where tId = " + str(execs[i][0])
                execute_query(db, sql)
                sql = "INSERT INTO trades (tExecid, tAccId, tConId, tTime, tShares, tPrice, tCommission, tLiquidation, "
                sql = sql + "toptPrice, toptIV, toptDelta, toptGamma, toptVega, toptTheta, toptPVDividend, toptPriceOfUnderlying, tActive) "
                # al nou registre, modifiquem l'Execid afegin-hi una C a davant, tActive=1 i tShares = execs[i][5]
                new_execid = 'C' + execs[i][1]
                sql = sql + "SELECT '" + new_execid + "',tAccId, tConId, tTime," + str(execs[i][3]) + ", tPrice, tCommission, tLiquidation, "
                sql = sql + "toptPrice, toptIV, toptDelta, toptGamma, toptVega, toptTheta, "
                sql = sql + "toptPVDividend, toptPriceOfUnderlying, 1 "  # active = 1
                sql = sql + "FROM trades WHERE tId = " + str(execs[i][0])
                execute_query(db, sql)
    except Exception as err:
        print(err)
        raise

# ATENCIÓ: S'HA DE MODIFICAR PERQUÈ EL DELETE TINGUI EN COMPTE l'ACCID
def dbfill_positions(db, execs):
    # execs[i] conté [ctId, ctAccId, ctExecId, ctConId, ctType, ctMultiplier, ctShares, ctPrice, ctDate, ctCommission, ctLiquidation, ctoptPrice, ctoptIV, ctoptDelta, ctoptGamma,
    # ctoptTheta, ctoptVega, ctoptPVDividend, ctoptPriceOfUnderlying, ctActive
    # Si ctActive='C', llavors execs[i][19]=[execs[j]], on execs[j] és l'execució que tanca execs[i]
    try:
        # borrem la taula de positions (per aquest Account) i després farem inserts del què tenim a execs. La taula positions_optiondetails es borra també (DELETE CASCADE)
        sql = "DELETE FROM positions WHERE pAccId = '" + str(execs[0][1]) + "'"
        count = execute_query(db, sql, commit = False)
        print('Deleted ' + str(count) + ' rows in table positions')
        dbcommit(db)
        for i in range(len(execs)):
            if execs[i][19] != 'D':
                print ('Inserting position ' + str(execs[i][0]))
                sql = "INSERT INTO positions (pId, pExecid, pAccId, pConId, pDate, pType, pMultiplier, pShares, pInitialPrice, pInitialValue, pCommission, pLiquidation, pActive) " \
                    + "SELECT ctId, ctExecId, ctAccId, ctConId, ctDate, ctType, ctMultiplier, ctShares, ctPrice, ctPrice*abs(ctShares)*ctMultiplier, ctCommission, ctLiquidation, ctActive " \
                    + "FROM combinedtrades WHERE ctID = " + str(execs[i][0])
                execute_query(db, sql)
                if execs[i][4] == 'OPT':
                    sql = "INSERT INTO positions_optiondetails (podId, podInitialModelPrice, podInitialIV, podInitialDelta, podInitialGamma, podInitialVega, " \
                        + "podInitialTheta, podInitialPVDividend, podInitialPriceOfUnderlying) " \
                        + "SELECT ctId, ctoptPrice, ctoptIV, ctoptDelta, ctoptGamma, ctoptVega, ctoptTheta, ctoptPVDividend, ctoptPriceOfUnderlying " \
                        + "FROM combinedtrades WHERE ctID = " + str(execs[i][0])
                    execute_query(db, sql)
                if execs[i][19] == 'C':
                    clist = execs[i][20]
                    sql = "UPDATE positions set pActive = %s, pClosingPrice = %s, pClosingValue = %s, pClosingDate = %s, pClosingId = %s, pPNL = %s, pCommission = %s, pLiquidation = %s " \
                        + "WHERE pId = " + str(execs[i][0])
                    pnl = -(execs[i][6]*execs[i][7] + clist[6]*clist[7])*execs[i][5]
                    val = (0, clist[7], clist[5] * abs(clist[6]) * clist[7], clist[8], clist[0], pnl, execs[i][9] + clist[9], clist[10])
                    execute_query(db, sql, values = val)
                    if execs[i][4] == 'OPT':
                        sql = "UPDATE positions_optiondetails set podFinalModelPrice = %s, podFinalIV = %s, podFinalDelta = %s, podFinalGamma = %s, podFinalTheta = %s,  " \
                            + "podFinalVega = %s, podFinalPVDividend = %s, podFinalPriceOfUnderlying = %s " \
                            + "WHERE podId = " + str(execs[i][0])
                        val = (clist[11], clist[12], clist[13], clist[14], clist[15], clist[16], clist[17], clist[18])
                        execute_query(db, sql, values = val)
    except Exception as err:
        print(err)
        raise


if __name__ == '__main__':

    myib = IB()
    mydb = dbconnect("localhost", "besuga", "xarnaus", "Besuga8888")
    #  creem instancia de connexió db al mateix temps que triem compte a IB amb el que operem
    acc = input("triar entre 'besugapaper' o 'xavpaper' ")
    if acc == "besugapaper":
        rslt = execute_query(mydb,"SELECT connHost, connPort, connAccId FROM connections WHERE connName = 'besugapaper7498'")
    elif acc == "xavpaper":
        rslt = execute_query(mydb, "SELECT connHost, connPort, connAccId FROM connections WHERE connName = 'xavpaper7497'")
    else:
        sys.exit("Unknown account!!")
    myib.connect(rslt[0][0], rslt[0][1], 1)
    myaccId = rslt[0][2]

    dbfill_contracts(mydb, get_contracts(myib))                     # inserta tots els diferents contractes, si no hi són a la DB
    dbfill_executions(mydb, get_executions(myib))                   # inserta les noves (de 1 a 7 dies) executions
    dbupdate_executions(mydb, dbanalyse_executions(mydb,myaccId))   # actualitza les executions per tancar les que toqui
    dbfill_positions(mydb,dbanalyse_positions(mydb, myaccId))       # borra totes les positions i les re-inserta


    dbdisconnect(mydb)
    # desconnectem de IBAPI
    myib.disconnect()
