


def diffdays(date1, date2):
    # entrem les dates en format 20181026, l'ordre de les datas és indiferent
    # caluulem la distància en dies entre les dues dates en termes absoluts
    from datetime import date, datetime
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
    # print(abs(delta.days))
    return abs(delta.days)


def accountAnalysis():
    accSum = myib.accountSummary()
    print(accSum)
    accountSummary = []
    for p in accSum:
        # print(p.tag, p.value)
        accountSummary.append((p.tag, p.value))
        dfaccountSummary = pd.DataFrame(accountSummary)
    print(dfaccountSummary)


# passem limit order
def tradelimitorder(contract, quantity, ordertype, price, trId):
    print("tradelimitorder")
    ultimaordre =[]
    order = LimitOrder(ordertype, quantity, price, tif="GTC", transmit=False)
    myib.qualifyContracts(contract)
    trade = myib.placeOrder(contract, order)
    myib.sleep(1)
    ultimaordre.append(ordertype)
    ultimaordre.append(quantity)
    ultimaordre.append(contract.symbol)
    ultimaordre.append(contract.secType)
    if contract.secType == "STK":
        pass
    elif contract.secType == "OPT":
        ultimaordre.append(contract.right)
        ultimaordre.append(contract.strike)
        ultimaordre.append(contract.lastTradeDateOrContractMonth)
    else:
        pass
    ultimaordre.append(price)
    ordrespassadeslist.append(ultimaordre)
    #print("ultimaordre   ",ultimaordre)
    #print("ordrespassadeslist   ",ordrespassadeslist)
    dbupdate_trades(mydb, trId)


# trunquem els decimals del preu per què IB accepti el preu
def formatPrice(price, prec):
    precision = prec
    newPrice = np.round(price, precision)
    price = newPrice
    return price


# determinem si hi ha un trade o no segons diferents criteris
def get_contracts(ib):
    # print("get contracts")
    pfl = ib.portfolio()
    lst = []
    for i in range(len(pfl)):
        contr = pfl[i].contract
        print(contr.localSymbol)
        ib.qualifyContracts(contr)
        lst2 = []
        lst2.append(contr.conId)  # lst2[0]
        lst2.append(contr.secType)  # lst2[1]
        lst2.append(contr.symbol)  # lst2[2]
        lst2.append(contr.localSymbol)  # lst2[3]
        lst2.append(contr.currency)  # lst2[4]
        lst2.append(contr.exchange)  # lst2[5]
        lst2.append(contr.tradingClass)
        if (contr.secType == 'OPT'):
            lst2.append(contr.lastTradeDateOrContractMonth)  # lst2[6]
            lst2.append(contr.strike)  # lst2[7]
            lst2.append(contr.right)  # lst2[8]
            lst2.append(contr.multiplier)  # lst2[9]
        elif (contr.secType == 'STK'):
            lst2.extend([None, None, None, None])
        lst.append(lst2)
    return (lst)


def get_trades(ib):
    fil = ib.fills()
    lst = []
    for f in fil:
        lst2 = []
        toptPrice = 0
        toptIV = 0
        toptDelta = 0
        toptGamma = 0
        toptVega = 0
        toptTheta = 0
        toptPVDividend = 0
        toptPriceOfUnderlying = 0
        tActive = 0
        dateentry = str(f.time)[0:4] + str(f.time)[5:7] + str(f.time)[8:10]
        lst2.append(f.execution.execId)
        lst2.append(f.execution.acctNumber)
        lst2.append(int(f.contract.conId))
        lst2.append(dateentry)
        if f.execution.side == "BOT":
            lst2.append(f.execution.shares)
        else:
            s = f.execution.shares
            lst2.append(-s)
        lst2.append(f.execution.price)
        lst2.append(f.commissionReport.commission)
        if f.execution.liquidation is None:
            lst2.append(0)
        else:
            lst2.append(f.execution.liquidation)
        lst2.append(toptPrice)
        lst2.append(toptIV)
        lst2.append(toptDelta)
        lst2.append(toptGamma)
        lst2.append(toptVega)
        lst2.append(toptTheta)
        lst2.append(toptPVDividend)
        lst2.append(toptPriceOfUnderlying)
        lst2.append(tActive)
        lst.append(lst2)
    # print("gettrades   lst",lst)
    return (lst)


def opendefensiveposition(cnt,pos):
    try:
        print("opendefensiveposition")
        # creem objectes tupus contracte
        stkcnt = Contract()  # el underlying de la opció
        optcnt1 = Contract()  # la opció que hi ha al portfolio
        optcnt2 = Contract()  # la potencial nova opció que es crearà

        # composem el contracte del underlying de la opció analitzada
        #stkcnt = Stock(cnt.symbol, "SMART", cnt.currency)
        stkcnt.symbol = cnt.symbol
        stkcnt.currency = cnt.currency
        stkcnt.secType = "STK"
        stkcnt.exchange ="SMART"
        myib.qualifyContracts(stkcnt)
        print("defensiveposition de: ",stkcnt)
        # composem el contracte de la opció analitzada
        optcnt1.conId = pos.conId
        myib.qualifyContracts(optcnt1)

        # composem la data d'expiració que és la mateixa tant per la opció original (optcnt1) com la nova defensiva (optcnt2)
        dateexpiration = str(optcnt1.lastTradeDateOrContractMonth)[0:4] + str(optcnt1.lastTradeDateOrContractMonth)[4:6] + str(optcnt1.lastTradeDateOrContractMonth)[6:8]
        print("dateexpiration   ",dateexpiration)

        # agafem lastprice del underlying provinent de ticker
        tstk = myib.reqTickers(stkcnt)
        topt1 = myib.reqTickers(optcnt1)
        lastpricestk = tstk[0].marketPrice()
        lastpriceopt1 = topt1[0].marketPrice()
        myib.sleep(1)

        # busquem la cadena d'opcions del underlying
        chains = myib.reqSecDefOptParams(stkcnt.symbol, '', stkcnt.secType, stkcnt.conId)
        chain = next(c for c in chains if c.tradingClass == stkcnt.symbol and c.exchange == 'SMART')

        #print(util.df(chains))
        print(util.df(chains))

        # separem strikes i expiracions
        lexps = []
        lstrikes = []
        lexps = chain.expirations
        lstrikes = chain.strikes
        myList = lstrikes
        lastpricestk = int(lastpricestk)

        # calculem la distància entre el preu del underlying ara i el strike de la opció venuda que estem analitzant
        strikedistance = abs(optcnt1.strike - lastpricestk)

        # busquem l'strike que més s'acosta al del preu actual del underlying
        orderstrike = min(lstrikes, key=lambda x: int(abs(int(x) - lastpricestk)))
        print("symbol  ",optcnt1.symbol,"strikedistance", strikedistance, "lastpricestk  ", lastpricestk, "orderstrike  ", orderstrike)

        # preparem el nou trade: si era un call ara un put...i al inreves
        if optcnt1.right == "C":
            opt2right = "P"
        else:
            opt2right = "C"

        # preparem el nou trade: qualifiquem la nova opció compensatoria
        optcnt2.symbol = optcnt1.symbol
        optcnt2.strike = orderstrike
        optcnt2.secType = optcnt1.secType
        optcnt2.exchange = "SMART"
        optcnt2.currency = optcnt1.currency
        optcnt2.right = opt2right
        optcnt2.lastTradeDateOrContractMonth = dateexpiration
        myib.qualifyContracts(optcnt2)
        print("optcon2", optcnt2)

        # busquem el preu al que cotitza la nova opció compensatoria
        topt2 = myib.reqTickers(optcnt2)
        lastpriceopt2bis = (topt2[0].bid + topt2[0].ask) / 2
        # lastprice = formatPrice(lastprice, 2)

        lastpriceopt2 = topt2[0].marketPrice()
        print("lastpriceopt2  ", lastpriceopt2, "lastpriceopt2bis   ", lastpriceopt2bis)
        myib.sleep(1)
        ordertype = ""
        # decidim si comprem o venem
        if pos.shares < 0:
            ordertype = 'SELL'
        else:
            ordertype = 'BUY'
        # executem la ordre
        print("tradelimitorder  ", optcnt2, abs(pos.shares), ordertype, lastpriceopt2, pos.conId)
        tradelimitorder(optcnt2, abs(pos.shares), ordertype, lastpriceopt2, optcnt2.conId)
        # tradelimitorder(cnt, abs(qty), orderType, abs(fmtprice), pos.conId)
    except Exception as err:
        print(err)
        raise


def allowTrade(pctpostimeelapsed, pctprofitnow,sectype):
    #print("allowtrade   ",pctpostimeelapsed, pctprofitnow,sectype)
    allowtrade = 0
    if sectype =="OPT":
        if pctpostimeelapsed <= 10 and pctprofitnow >30:
            allowtrade = 1
        if pctpostimeelapsed <= 20 and pctprofitnow >40:
            allowtrade = 1
        if pctpostimeelapsed <= 50 and pctprofitnow > 65:
            allowtrade = 1
        if pctpostimeelapsed <= 75 and pctprofitnow >75:
            allowtrade = 1
        if pctprofitnow >= pctprofittarget:
            allowtrade = 1
        if pctprofitnow <= -75:
            allowtrade = 2
    elif sectype =="STK":
        if pctprofitnow >= 20:
            allowtrade = 3
        if pctprofitnow <= -20:
            allowtrade = 4
    else:
        allowtrade = 0
    return allowtrade


def processopenpositions():
    print("processopenpositions")
    try:
        # llegim posicions obertes de la base de dades
        query = "SELECT pId, pExecId, pAccId, pConId, pDate, pType, pMultiplier, pShares, pInitialPrice,pInitialValue, pClosingPrice, pClosingValue," \
                " pClosingDate, pClosingId, pPNL, pCommission, pLiquidation, pActive" \
                " FROM positions LEFT JOIN contracts ON positions.pConId = contracts.kConId" \
                " WHERE pAccId =  '" + vAccId + "' AND pActive = 1"

        rst = execute_query(mydb, query, values=None)
        # definim namedtuple "positions" per a processar posicions obertes
        positions = namedtuple("positions", "Id execId accId conId  \
                                     date type multiplier shares initialPrice initialValue closingPrice \
                                     closingValue closingDate closingId PNL commission liquidation \
                                     active")        
        
        # passem les execucions obertes en forma de namedtuple a la llista "openpos"
        #ordrespassadeslist = []
        openpos = []
        for i in range(len(rst)):
            position = positions(Id=rst[i][0], execId=rst[i][1], accId=rst[i][2], conId=rst[i][3],
                           date=rst[i][4], type=rst[i][5], multiplier=rst[i][6], shares=rst[i][7],
                           initialPrice=rst[i][8], initialValue=rst[i][9], closingPrice=rst[i][10],
                           closingValue=rst[i][11], closingDate=rst[i][12], closingId=rst[i][13],
                           PNL=rst[i][14], commission=rst[i][15],
                           liquidation=rst[i][16], active=rst[i][17])
            openpos.append(position)

        print ("openpos",openpos)

        # llegim "openpos" en forma de loop per a decidir què fer amb cada execució oberta

        pctProfitList = []
        for pos in openpos:
            #creem un objecte Contract
            cnt = Contract()
            #fem una instancia de contract amb el contracte llegit del query de trades oberts de la db trades
            cnt.conId = pos.conId

            myib.qualifyContracts(cnt)
            pfl = myib.portfolio()



            # obtenim i formategem data expiració
            dateexpiration = str(cnt.lastTradeDateOrContractMonth)[0:4] + str(cnt.lastTradeDateOrContractMonth)[4:6] + str(cnt.lastTradeDateOrContractMonth)[6:8]


            # agafem lastprice provinent de ticker
            # ticker = myib.reqTickers(cnt)
            # myib.sleep(1)
            # lastprice = (ticker[0].bid + ticker[0].ask) / 2
            # lastprice = formatPrice(lastprice, 2)
            # print("tickerbid  ", ticker[0].bid, "  tickerask  ",ticker[0].ask, " lastprice  ", lastprice)

            # agafem lastprice provinent de portfolio
            lastprice = 0
            for f in pfl:
                if pos.conId == f.contract.conId:
                    lastprice = f.marketPrice
                    #lastprice = f.marketValue
            # demanem dades a traves de reqMktData
            # m_data = myib.reqMktData(cnt)
            # while m_data.last != m_data.last: myib.sleep(0.01)  # Wait until data is in.
            # myib.cancelMktData(cnt)
            # print("m_data   ",m_data.last)

            avgcost = float(pos.initialPrice)
            vshares = pos.shares
            # calculem pctprofitnow (el pnl de la posició)
            if vshares < 0:
                pctprofitnow = (1 - (lastprice / avgcost)) * 100
            else:
                pctprofitnow = ((lastprice / avgcost) - 1) * 100
            print(cnt.symbol,"  ",vshares, "lastprice   ",lastprice,"avgcost",avgcost,"pctprofitnow  ",pctprofitnow)

            # calculem percentatge temps passat entre apertura posició i expiració per a posicions d'opcions
            pctpostimeelapsed = 0
            if cnt.secType == "OPT":
                dateentry = str(pos.date)[0:4] + str(pos.date)[5:7] + str(pos.date)[8:10]
                datetoday = datetime.datetime.now().strftime("%Y%m%d")
                datedifffromentry = diffdays(dateentry, dateexpiration)
                datedifffromtoday = diffdays(datetoday, dateexpiration)
                pctpostimeelapsed = int((1 - datedifffromtoday / datedifffromentry) * 100)



            # d'acord amb els paràmetres calculats decidim si es fa un trade o no a la funció "allowtrade"
            #allowtrade = allowTrade(pctpostimeelapsed, pctprofitnow)
            allowtrade  = allowTrade(pctpostimeelapsed, pctprofitnow,cnt.secType)
            #allowtrade = 0
            pctProfitList.append(
                [cnt.symbol, pos.shares, cnt.right, cnt.strike, pos.initialPrice, lastprice, int(pctprofitnow),
                 pctpostimeelapsed, allowtrade])

            # allowtrade = 1 tancar posició per recollida de beneficis, allowtrade = 2 fem un trade defensio de la posició
            if allowtrade == 1:
                if pos.shares < 0:
                    ordertype = 'BUY'
                else:
                    ordertype = 'SELL'
                # Configurem preu operació
                if ordertype == "BUY" and cnt.secType == "OPT":
                    # price = ((avgcost * ((100 - pctprofitnow)) / 100)) / 100
                    price = avgcost * (1 - pctprofitnow / 100)
                elif ordertype == "SELL" and cnt.secType == "OPT":
                    # price = (avgcost * (1 + (pctprofitnow / 100))) / 100
                    price = avgcost * (1 + pctprofitnow / 100)
                fmtprice = formatPrice(price, 2)
                tradelimitorder(cnt, abs(vshares), ordertype, abs(fmtprice), pos.conId)
            elif allowtrade == 2:
                # obrim posició defensiva
                opendefensiveposition(cnt,pos)
            elif allowtrade == 3:
                if pos.shares < 0:
                    ordertype = 'BUY'
                else:
                    ordertype = 'SELL'
                # Configurem preu operació
                if ordertype == "BUY":
                    price = lastprice
                    #price = avgcost * (1 - pctprofitnow / 100)
                elif ordertype == "SELL":
                    price = lastprice
                    #price = avgcost * (1 + pctprofitnow / 100)
                fmtprice = formatPrice(price, 2)
                tradelimitorder(cnt, abs(vshares), ordertype, abs(fmtprice), pos.conId)
            elif allowtrade == 4:
                if pos.shares < 0:
                    ordertype = 'BUY'
                else:
                    ordertype = 'SELL'
                # Configurem preu operació
                if ordertype == "BUY":
                    price = lastprice
                    #price = avgcost * (1 - pctprofitnow / 100)
                elif ordertype == "SELL":
                    price = lastprice
                    #price = avgcost * (1 + pctprofitnow / 100)
                fmtprice = formatPrice(price, 2)
                tradelimitorder(cnt, abs(vshares), ordertype, abs(fmtprice), pos.conId)
            elif allowtrade == "8888":
                # PLACE  MAEKET ORDER
                # MarketOrder(cnt, abs(vshares), ordertype, abs(fmtprice), pos.conId)
                #Order = MarketOrder(ordertype,abs(vshares))
                #trade = myib.placeOrder(cnt,Order)
                pass
            else:
                pass

        print("pctProfitList             ", pd.DataFrame(pctProfitList))
        print("ordrespassadeslist        ", pd.DataFrame(ordrespassadeslist))
    except Exception as err:
        print(err)
        raise


if __name__ == "__main__":
    # importem llibreries
    import ib_insync
    import numpy as np
    from ib_insync import *
    import openpyxl as op
    import pandas as pd
    import os
    import datetime
    import tkinter as tk
    from tkinter import messagebox as msgbox
    from collections import OrderedDict
    from collections import namedtuple
    import mysql.connector
    from mysql.connector import errorcode
    from numpy import sign

    # inicialització paràmeteres
    global ordrespassadeslist
    ordrespassadeslist =[]
    pctprofittarget = 80

    myib = IB()
    mydb = dbconnect("localhost", "besuga", "xarnaus", "Besuga8888")

    #  creem instancia de connexió db al mateix temps que triem compte a IB amb el que operem
    acc = input("triar entre 'besugapaper' o 'xavpaper' \n")
    if acc == "besugapaper":
        rslt = execute_query(mydb,
                             "SELECT connHost, connPort, connAccId FROM connections WHERE connName = 'besugapaper7498'")
    elif acc == "xavpaper":
        rslt = execute_query(mydb,
                             "SELECT connHost, connPort, connAccId FROM connections WHERE connName = 'xavpaper7497'")
    else:
        sys.exit("Unknown account!!")
    myib.connect(rslt[0][0], rslt[0][1], 1)
    myaccId = rslt[0][2]

    #demanem delayed data
    myib.reqMarketDataType(4)
    #analitzem Accoount
    accountAnalysis()

    # analitzem posicions obertes
    #portfolio_analysis()


    # processem trades oberts, tancant els que calgui i obrint trades defensius si cal
    processopenpositions()


    # selectoptioncontract()

    # desconnectem de IBAPI
    ibDisconnect()

