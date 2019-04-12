# Standard library imports
import sys
import datetime

# Third party imports
import ib_insync as ibsync
import numpy as np
from collections import namedtuple

# Local application imports
from besuga_ib_utilities import error_handling
from besuga_ib_utilities import execute_query
from besuga_ib_utilities import tradelimitorder
from besuga_ib_utilities import diffdays
from besuga_ib_utilities import formatPrice
import besuga_ib_utilities as ibutil
import ib_config as ibconfig


def opendefensiveposition(ib, cnt, pos):
    try:
        print("opendefensiveposition")
        # creem objectes tupus contracte 
        stkcnt = ibsync.Contract()  # el underlying de la opció
        optcnt1 = ibsync.Contract()  # la opció que hi ha al portfolio
        optcnt2 = ibsync.Contract()  # la potencial nova opció que es crearà

        # composem el contracte del underlying de la opció analitzada
        # stkcnt = Stock(cnt.symbol, "SMART", cnt.currency)
        stkcnt.symbol = cnt.symbol
        stkcnt.currency = cnt.currency
        stkcnt.secType = "STK"
        stkcnt.exchange = "SMART"
        ib.qualifyContracts(stkcnt)
        print("defensiveposition de: ", stkcnt)
        # composem el contracte de la opció analitzada
        optcnt1.conId = pos.conId
        ib.qualifyContracts(optcnt1)

        # composem la data d'expiració que és la mateixa tant per la opció original (optcnt1) com la nova defensiva (optcnt2)
        dateexpiration = str(optcnt1.lastTradeDateOrContractMonth)[0:4] + str(optcnt1.lastTradeDateOrContractMonth)[
                                                                          4:6] + str(
            optcnt1.lastTradeDateOrContractMonth)[6:8]

        # agafem lastprice del underlying provinent de ticker
        tstk = ib.reqTickers(stkcnt)
        topt1 = ib.reqTickers(optcnt1)
        lastpricestk = tstk[0].marketPrice()
        lastpriceopt1 = topt1[0].marketPrice()
        ib.sleep(1)

        # busquem la cadena d'opcions del underlying
        chains = ib.reqSecDefOptParams(stkcnt.symbol, '', stkcnt.secType, stkcnt.conId)
        chain = next(c for c in chains if c.tradingClass == stkcnt.symbol and c.exchange == 'SMART')

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
        ib.qualifyContracts(optcnt2)
        print("optcon2", optcnt2)

        # busquem el preu al que cotitza la nova opció compensatoria
        topt2 = ib.reqTickers(optcnt2)
        lastpriceopt2bis = (topt2[0].bid + topt2[0].ask) / 2
        # lastprice = formatPrice(lastprice, 2)

        lastpriceopt2 = topt2[0].marketPrice()
        ib.sleep(1)
        # executem la ordre
        print("opendefensiveposition - ", optcnt2, pos.shares, lastpriceopt2, pos.conId)
        tradelimitorder(ib, optcnt2, pos.shares, lastpriceopt2)
    except Exception as err:
        error_handling(err)
        raise


def allowTrade(pctpostimeelapsed, pctprofitnow, sectype):
    try:
        allowtrade = 0
        if sectype == "OPT":
            if pctpostimeelapsed <= 10 and pctprofitnow > ibconfig.myoptprofit10: allowtrade = 1
            if pctpostimeelapsed <= 20 and pctprofitnow > ibconfig.myoptprofit20: allowtrade = 1
            if pctpostimeelapsed <= 50 and pctprofitnow > ibconfig.myoptprofit50: allowtrade = 1
            if pctpostimeelapsed <= 75 and pctprofitnow > ibconfig.myoptprofit75: allowtrade = 1
            if pctprofitnow >= ibconfig.myoptprofit: allowtrade = 1
            if pctprofitnow <= ibconfig.myoptloss: allowtrade = 2
        elif sectype == "STK":
            if pctprofitnow >= ibconfig.mystkprofit: allowtrade = 3
            if pctprofitnow <= ibconfig.mystkloss: allowtrade = 4
        else:
            allowtrade = 0
        return allowtrade
    except Exception as err:
        error_handling(err)
        raise


def processopenpositions(ib, db, vAccId):
    print("\nprocessopenpositions")
    try:

        # llegim posicions obertes de la base de dades
        query = "SELECT pId, pExecId, pAccId, pConId, pDate, pType, pMultiplier, pShares, pInitialPrice,pInitialValue, pClosingPrice, pClosingValue," \
                " pClosingDate, pClosingId, pPNL, pCommission, pLiquidation, pActive" \
                " FROM positions LEFT JOIN contracts ON positions.pConId = contracts.kConId" \
                " WHERE pAccId =  '" + vAccId + "' AND pActive = 1"

        rst = execute_query(db, query, values=None)
        # definim namedtuple "positions" per a processar posicions obertes
        positions = namedtuple("positions", "Id execId accId conId  \
                                     date type multiplier shares initialPrice initialValue closingPrice \
                                     closingValue closingDate closingId PNL commission liquidation \
                                     active")

        # passem les execucions obertes en forma de namedtuple a la llista "openpos"
        openpos = []
        for i in range(len(rst)):
            position = positions(Id=rst[i][0], execId=rst[i][1], accId=rst[i][2], conId=rst[i][3],
                                 date=rst[i][4], type=rst[i][5], multiplier=rst[i][6], shares=rst[i][7],
                                 initialPrice=rst[i][8], initialValue=rst[i][9], closingPrice=rst[i][10],
                                 closingValue=rst[i][11], closingDate=rst[i][12], closingId=rst[i][13],
                                 PNL=rst[i][14], commission=rst[i][15],
                                 liquidation=rst[i][16], active=rst[i][17])
            openpos.append(position)
        # llegim "openpos" en forma de loop per a decidir què fer amb cada execució oberta
        pctProfitList = []
        for pos in openpos:
            # creem un objecte Contract
            cnt = ibsync.Contract()
            # fem una instancia de contract amb el contracte llegit del query de trades oberts de la db trades
            cnt.conId = pos.conId

            ib.qualifyContracts(cnt)
            pfl = ib.portfolio()

            # obtenim i formategem data expiració
            dateexpiration = str(cnt.lastTradeDateOrContractMonth)[0:4] + str(cnt.lastTradeDateOrContractMonth)[
                                                                          4:6] + str(cnt.lastTradeDateOrContractMonth)[
                                                                                 6:8]

            # agafem lastprice provinent de portfolio
            lastprice = 0
            for f in pfl:
                if pos.conId == f.contract.conId:
                    lastprice = f.marketPrice
                    # lastprice = f.marketValue
            # demanem dades a traves de reqMktData
            # m_data = ib.reqMktData(cnt)
            # while m_data.last != m_data.last: ib.sleep(0.01)  # Wait until data is in.
            # ib.cancelMktData(cnt)
            # print("m_data   ",m_data.last)

            avgcost = float(pos.initialPrice)
            vshares = pos.shares
            # calculem pctprofitnow (el pnl de la posició)
            if vshares < 0:
                pctprofitnow = (1 - (lastprice / avgcost)) * 100
            else:
                pctprofitnow = ((lastprice / avgcost) - 1) * 100
            print(cnt.symbol, "  ", vshares, "lastprice   ", lastprice, "avgcost", avgcost, "pctprofitnow  ",
                  pctprofitnow)

            # calculem percentatge temps passat entre apertura posició i expiració per a posicions d'opcions
            pctpostimeelapsed = 0
            if cnt.secType == "OPT":
                dateentry = str(pos.date)[0:4] + str(pos.date)[5:7] + str(pos.date)[8:10]
                datetoday = datetime.datetime.now().strftime("%Y%m%d")
                datedifffromentry = diffdays
                datedifffromentry = diffdays(dateentry, dateexpiration)
                datedifffromtoday = diffdays(datetoday, dateexpiration)
                pctpostimeelapsed = int((1 - datedifffromtoday / datedifffromentry) * 100)

            # d'acord amb els paràmetres calculats decidim si es fa un trade o no a la funció "allowtrade"
            # allowtrade = allowTrade(pctpostimeelapsed, pctprofitnow)
            allowtrade = allowTrade(pctpostimeelapsed, pctprofitnow, cnt.secType)
            # allowtrade = 0
            pctProfitList.append(
                [cnt.symbol, pos.shares, cnt.right, cnt.strike, pos.initialPrice, lastprice, int(pctprofitnow),
                 pctpostimeelapsed, allowtrade])

            # allowtrade = 1 tancar posició per recollida de beneficis, allowtrade = 2 fem un trade defensio de la posició
            price = 0
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
                print("Close Position: \t", cnt, "\t", ordertype, "\t", fmtprice)
                tradelimitorder(ib, cnt, -vshares, fmtprice)
            elif allowtrade == 2:
                # obrim posició defensiva
                opendefensiveposition(ib, cnt, pos)
            elif allowtrade == 3:
                if pos.shares < 0:
                    ordertype = 'BUY'
                else:
                    ordertype = 'SELL'
                # Configurem preu operació
                if ordertype == "BUY":
                    price = lastprice
                    # price = avgcost * (1 - pctprofitnow / 100)
                elif ordertype == "SELL":
                    price = lastprice
                    # price = avgcost * (1 + pctprofitnow / 100)
                fmtprice = formatPrice(price, 2)
                print("Close Position: \t", cnt, "\t", ordertype, "\t", fmtprice)
                tradelimitorder(ib, cnt, -vshares, fmtprice)
            elif allowtrade == 4:
                if pos.shares < 0:
                    ordertype = 'BUY'
                else:
                    ordertype = 'SELL'
                # Configurem preu operació
                if ordertype == "BUY":
                    price = lastprice
                    # price = avgcost * (1 - pctprofitnow / 100)
                elif ordertype == "SELL":
                    price = lastprice
                    # price = avgcost * (1 + pctprofitnow / 100)
                fmtprice = formatPrice(price, 2)
                print("Close Position: \t", cnt, "\t", ordertype, "\t", fmtprice)
                tradelimitorder(ib, cnt, -vshares, fmtprice)
            elif allowtrade == "8888":
                pass
            else:
                pass
    except Exception as err:
        error_handling(err)
        raise


if __name__ == "__main__":

    myib = ibsync.IB()
    mydb = ibutil.dbconnect("localhost", "besuga", "xarnaus", "Besuga8888")
    acc = input("triar entre 'besugapaper', 'xavpaper', 'mavpaper1', 'mavpaper2'")
    if acc == "besugapaper":
        rslt = execute_query(mydb,
                             "SELECT connHost, connPort, connAccId FROM connections WHERE connName = 'besugapaper7498'")
    elif acc == "xavpaper":
        rslt = execute_query(mydb,
                             "SELECT connHost, connPort, connAccId FROM connections WHERE connName = 'xavpaper7497'")
    elif acc == "mavpaper1":
        rslt = execute_query(mydb, "SELECT connHost, connPort, connAccId FROM connections WHERE connName = 'mavpaper1'")
    elif acc == "mavpaper2":
        rslt = execute_query(mydb, "SELECT connHost, connPort, connAccId FROM connections WHERE connName = 'mavpaper2'")
    else:
        sys.exit("Unknown account!!")
    myib.connect(rslt[0][0], rslt[0][1], 1)
    myaccId = rslt[0][2]
    myordersdict = {}

    # demanem delayed data
    myib.reqMarketDataType(4)

    # processem trades oberts, tancant els que calgui i obrint trades defensius si cal
    processopenpositions(myib, mydb, myaccId)


    ibutil.dbdisconnect(mydb)
    # desconnectem de IBAPI
    myib.disconnect()

