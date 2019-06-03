# Standard library imports 
import sys
import builtins
from itertools import groupby
from datetime import date, timedelta

# Third party imports
import ib_insync as ibsync
import xmltodict

# Local application imports
from besuga_ib_utilities import error_handling
from besuga_ib_utilities import execute_query
from besuga_ib_utilities import formatPrice
import besuga_ib_utilities as ibutil
import besuga_ib_manage_db_positions as ibdb
import besuga_ib_config as cf


# Triem quin scan volem córrer. Si només en fem córrer 1, codi = descripció.
# Per fer-ne anar + de 1, s'ha d'afegir a la BD-scancodes amb fromat scode: zzzz sdescriptions =[code1, AND, code 2, OR, code 3]
# AND i OR són operadors lògics. Sempre va d'esquerra a dreta, no admet parèntesis
def scanselection (db):
    sql = "SELECT scode, ' - ', sdescription FROM scancodes ORDER BY scode"
    rslt = ibutil.execute_query(db, sql)
    text = str(rslt).strip('[]').replace("'", '').replace(",", '').replace('(', '').replace(')', '\n')
    scancode = input("triar els scans desitjats - exit per sortir: \n " + text)
    while scancode != "exit":
        if scancode in (item[0] for item in rslt):
            print("\nScan escollit: ", scancode, "\n")
            break
        else:
            print("Scan desconegut!")
            scancode = input("")
    if scancode == "exit": sys.exit("Exit requested!")
    scanselection = [scancode]+ [item[2] for item in rslt if item[0] == scancode]
    return scanselection                    # [code, description]


# Torna una llista d'stocks depenent de la llista d'scans i de l'operador lògic (AND, OR o ``)
# scannerparms = [instrument, locationCode, scanCode, aboveVolume, marketCapAbove, averageOptionVolumeAbove]
# maxstocke = màximum number of stocke returned by the scan
def getscannedstocks(ib, scandesc):
    '''
    # atttibutes of the scannerSubscription object, they can be used to filter for some conditions

    NumberOfRows[get, set] #   int, The number of rows to be returned for the query
    Instrument[get, set] # string, The instrument's ty for the scan (STK, FUT, HK, etc.)
    LocationCode[get, set] # string, The request's location (STK.US, STK.US.MAJOR, etc.)
    ScanCode[get, set] # string, Same as TWS Market Scanner's "parameters" field, i.e. TOP_PERC_GAIN
    AbovePrice[get, set] # double, Filters out contracts which price is below this value
    BelowPrice[get, set] # double, Filters out contracts which price is above this value
    AboveVolume[get, set] # int, Filters out contracts which volume is above this value
    AverageOptionVolumeAbove[get, set] # int, Filteres out Cotracts which option volume is above this value
    MarketCapAbove[get, set] # double, Filters out Contracts which market cap is above this value.
    MarketCapBelow[get, set] # double, Filters out Contracts which market cap is below this value.
    MoodyRatingAbove[get, set] # string, Filters out Contracts which Moody 's rating is below this value.
    MoodyRatingBelow[get, set] # string, Filters out Contracts which Moody 's rating is above this value.
    SpRatingAbove[get, set] # string, Filters out Contracts with a S & P rating below this value.
    SpRatingBelow[get, set] # string, Filters out Contracts with a S & P rating below this value.
    MaturityDateAbove[get, set] # string, Filter out Contracts with a maturity date earlier than this value.
    MaturityDateBelow[get, set] # string, Filter out Contracts with a maturity date older than this value.
    CouponRateAbove[get, set] # double, Filter out Contracts with a coupon rate lower than this value.
    CouponRateBelow[get, set] # double, Filter out Contracts with a coupon rate higher than this value.
    ExcludeConvertible[get, set] # bool, Filters out Convertible bonds.
    ScannerSettingPairs[get, set] # string, For example, a pairing "Annual, true" used on the "top Option Implied Vol % Gainers" scan would return annualized volatilities.
    StockTypeFilter[get, set] # string

    # list of instruments of the scannerSubscription object
    "STK",
    "STOCK.HK",
    "STOCK.EU",
    "STK.US",
    # list of location codes of scannerSubscription object
    "STK.US.MAJOR",
    "STK.US.MINOR",
    "STK.HK.SEHK",
    "STK.HK.ASX",
    "STK.EU"

    # list of scanCodes of the scannerSubscription object

    "LOW_OPT_VOL_PUT_CALL_RATIO",
    "HIGH_OPT_IMP_VOLAT_OVER_HIST",
    "LOW_OPT_IMP_VOLAT_OVER_HIST",
    "HIGH_OPT_IMP_VOLAT",
    "TOP_OPT_IMP_VOLAT_GAIN",
    "TOP_OPT_IMP_VOLAT_LOSE",
    "HIGH_OPT_VOLUME_PUT_CALL_RATIO",
    "LOW_OPT_VOLUME_PUT_CALL_RATIO",
    "OPT_VOLUME_MOST_ACTIVE",
    "HOT_BY_OPT_VOLUME",
    "HIGH_OPT_OPEN_INTEREST_PUT_CALL_RATIO",
    "LOW_OPT_OPEN_INTEREST_PUT_CALL_RATIO",
    "TOP_PERC_GAIN",
    "MOST_ACTIVE",
    "TOP_PERC_LOSE",
    "HOT_BY_VOLUME",
    "TOP_PERC_GAIN",
    "HOT_BY_PRICE",
    "TOP_TRADE_COUNT",
    "TOP_TRADE_RATE",
    "TOP_PRICE_RANGE",
    "HOT_BY_PRICE_RANGE",
    "TOP_VOLUME_RATE",
    "LOW_OPT_IMP_VOLAT",
    "OPT_OPEN_INTEREST_MOST_ACTIVE",
    "NOT_OPEN",
    "HALTED",
    "TOP_OPEN_PERC_GAIN",
    "TOP_OPEN_PERC_LOSE",
    "HIGH_OPEN_GAP",
    "LOW_OPEN_GAP",
    "LOW_OPT_IMP_VOLAT",
    "TOP_OPT_IMP_VOLAT_GAIN",
    "TOP_OPT_IMP_VOLAT_LOSE",
    "HIGH_VS_13W_HL",
    "LOW_VS_13W_HL",
    "HIGH_VS_26W_HL",
    "LOW_VS_26W_HL",
    "HIGH_VS_52W_HL",
    "LOW_VS_52W_HL",
    "HIGH_SYNTH_BID_REV_NAT_YIELD",
    "LOW_SYNTH_BID_REV_NAT_YIELD"

    '''
    try:
        print("\n\t getscannedstocks ")
        scandesc = scandesc.split()                   #scandesc = scancode1 AND scancode2 OR,....
        scan = ibsync.ScannerSubscription(instrument=cf.myscaninstrument, locationCode=cf.myscanlocation, scanCode = scandesc[0], aboveVolume=cf.myscanvolabove,
                                          marketCapAbove=cf.myscanmktcapabove, averageOptionVolumeAbove=cf.myscanavgvoloptabove)
        scanner = ib.reqScannerData(scan, [])
        # stklst[i] és una llista de Contracts
        stklst = []
        for stock in scanner[:cf.myscanmaxstocks]:              # loops through stocks in the scanner
            contr = stock.contractDetails.contract
            ib.qualifyContracts(contr)
            stklst.append(contr)                        # stklst llista de Contracts
        for i in range(1, len(scandesc), 2):
            scan = ibsync.ScannerSubscription(instrument=cf.myscaninstrument, locationCode=cf.myscanlocation,
                    scanCode=scandesc[i+1], aboveVolume=cf.myscanvolabove, marketCapAbove=cf.myscanmktcapabove,
                    averageOptionVolumeAbove=cf.myscanavgvoloptabove)
            scanner = ib.reqScannerData(scan, [])
            for stock in scanner[:cf.myscanmaxstocks]:           # loops through stocks in the scanner
                contr = stock.contractDetails.contract
                ib.qualifyContracts(contr)
                stklst.append(contr)
            if scandesc[i] == 'OR':
                stklst = list(set(stklst))                 # treiem els duplicats
            elif scandesc[i] == "AND":
                stklst = [key for key, group in groupby(stklst) if len(list(group)) > 1]      # mantenim només els duplicats doncs són la intersecció
                print("\n\t getscannedstocks ", stklst)
            else:
                raise Exception ("Hi ha alguna cosa que no funciona amb la descripció de l'scan code")
        return stklst
    except Exception as err:
        #error_handling(err)
        raise


def fillfundamentals(ib, stklst):
    print("\n\t fillfundamentals")
    try:
        # convertim stklist en una llista de llistes stklst = [[cnt]] per poder-hi fer cabre els fundamentals
        stklst = [[a] for a in stklst]
        for i in range(len(stklst)):
            cnt = stklst[i][0]
            fr = ib.reqMktData(cnt, "258")
            ib.sleep(10)
            aux = dict(t.split('=') for t in str(fr.fundamentalRatios)[18:-1].split(',') if t)
            fratios = {key.lstrip(): value for key, value in aux.items()}
            addfunds = requestadditionalfundamentals(ib, cnt)
            # we fill the list with fundamental data that we will use to update database + make computations to select
            # candidates to open positions
            if fratios != None:
                stklst[i].append(fratios.get("AFEEPSNTM", ""))              # stklst[i][1]
                stklst[i].append(fratios.get("Frac52Wk", ""))               # fraction of 52 week high/low - stklst[i][2]
                stklst[i].append(fratios.get("BETA", ""))                   # stklst[i][3]
                stklst[i].append(fratios.get("APENORM", ""))                # annual normalized PE - stklst[i][4]
                stklst[i].append(fratios.get("QTOTD2EQ", ""))               # total debt/total equity - stklst[i][5]
                stklst[i].append(fratios.get("EV2EBITDA_Cur", ""))          # Enterprise value/ebitda - TTM  - stklst[i][6]
                stklst[i].append(fratios.get("TTMPRFCFPS", ""))             # price to free cash flow per share - TTM  - stklst[i][7]
                stklst[i].append(fratios.get("YIELD", ""))                  # Dividend yield - stklst[i][8]
                stklst[i].append(fratios.get("TTMROEPCT", ""))              # return on equity % - stklst[i][9]
            else:
                stklst[i].extend([0, 0, 0, 0, 0, 0, 0, 0, 0])
                '''
                Not used attributes????
                vcurrency = fratios.get("CURRENCY", "")
                vhigh52wk = fratios.get("NHIG", "")  # 52 week high
                vlow52wk = fratios.get("NLOW", "")  # 53 week low
                vpeexclxor = fratios.get("PEEXCLXOR", "")  # annual PE excluding extraordinary items
                vevcur = fratios.get("EV-Cur", "")  # Current enterprise value
                '''
            if addfunds != None:
                stklst[i].append(addfunds["TargetPrice"])                   # stklst[i][10]
                stklst[i].append(addfunds["ConsRecom"])                     # stklst[i][11]
                stklst[i].append(addfunds["ProjEPS"])                       # stklst[i][12]
                stklst[i].append(addfunds["ProjEPSQ"])                      # stklst[i][13]
                stklst[i].append(addfunds["ProjPE"])                        # stklst[i][14]
            else:
                stklst[i].extend([0, 0, 0, 0, 0])
            for j in range(1, len(stklst[i])):
                if stklst[i][j] == '': stklst[i][j] = 0
                if stklst[i][j] == 'nan': stklst[i][j] = 0
                if stklst[i][j] is None: stklst[i][j] = 0
            print("fillfundamentals ", stklst[i])
        return (stklst)
    except Exception as err:
        #error_handling(err)
        raise


def processpreselectedstocks(ib, db, accid, stklst, scancode):
    print("\n\t processpreselectedstocks")
    try:
        listorders = []
        for i in range(len(stklst)):
            cnt = stklst[i][0]                  # contract
            targetprice = stklst[i][10]         # target price
            frac52w = stklst[i][2]              # distància a la que està del high/low
            # si no té historial (fdate = Now només) no faig res
            # tampoc faig res si la Earnings Date és massa propera
            sql =  "SELECT cfs.fTargetPrice FROM contracts c "+\
                " RIGHT JOIN contractfundamentals cfs on c.kConId = cfs.fConId "+\
                " WHERE cfs.fConId = '" + str(cnt.conId) + "' AND cfs.fAccId = '" + str(accid) + "' AND cfs.fDate < DATE(NOW()) "+\
                " AND DATEDIFF(c.kEarningsDate, DATE(NOW())) >  " + str(cf.mydaystoearnings) +\
                " ORDER BY cfs.fDate DESC LIMIT 1 "
            rst = execute_query(db, sql)
            if rst != [] and targetprice > 0:
                # si scancode = HIGH_VS_52W_HL i la distància al hign és <= que un 1%
                # i TargetPrice > el que està guardat a la base de dades que no és d'avui
                if scancode == 'HIGH_VS_52W_HL' and float(frac52w) >= cf.my52whighfrac and targetprice > rst[0][0]:
                    print("Open new LOW_VS_52W_HL -  Put ", cnt.symbol)
                    listorders.append(opennewoption(ib, db, cnt, "SELL", "P", cf.myoptdaystoexp, scancode))
                elif scancode == 'LOW_VS_52W_HL' and float(frac52w) <= cf.my52wlowfrac and targetprice < rst[0][0]:
                    print("Open new LOW_VS_52W_HL -  Call ", cnt.symbol)
                    listorders.append(opennewoption(ib, db, cnt, "SELL", "C", cf.myoptdaystoexp, scancode))
                else:
                    print("No action required for Stock:   ", cnt.conId, ' ', cnt.symbol,
                        "Scan Code: ", stklst[i][0], "frac52w: ", frac52w, " New Target Price: ", targetprice,
                          "Old Target Price: ", rst[0][0])
            elif targetprice <= 0:
                print("No target price for Stock ", cnt.conId, ' ', cnt.symbol)
            else:
                print ("No DB history for stock: ", cnt.conId, ' ', cnt.symbol, "\t- Scan Code: ", stklst[i][0])
        return listorders
    except Exception as err:
        #error_handling(err)
        raise


def requestadditionalfundamentals(ib, cnt):
    try:
        fundamentals = ib.reqFundamentalData(cnt, 'ReportSnapshot')
        if fundamentals != []:
            doc = xmltodict.parse(fundamentals)
            ib.sleep(2)
            dictratios ={}
            for i in range(len(doc['ReportSnapshot']['ForecastData']['Ratio'])):
                dkey=(doc['ReportSnapshot']['ForecastData']['Ratio'][i]['@FieldName'])
                dvalue=(doc['ReportSnapshot']['ForecastData']['Ratio'][i]['Value']['#text'])
                dvalue = dvalue.split(".")
                if len(dvalue) == 1:
                    dvalue.append(0)
                dvalue[1]= "0."+str(dvalue[1])
                dvalue = float(dvalue[0]) + float(dvalue[1])
                dvalue = round(dvalue, 2)
                dictratios[dkey]=dvalue
            return(dictratios)
    except Exception as err:
        #error_handling(err)
        raise


def opennewoption(ib, db, cnt, opttype, optright, optdaystoexp, scancode):
    print("\n\t opennewoption")
    try:
        # agafem lastprice del underlying provinent de ticker
        lastpricestk = ib.reqTickers(cnt)[0].marketPrice()
        # busquem la cadena d'opcions del underlying
        chains = ib.reqSecDefOptParams(cnt.symbol, '', cnt.secType, cnt.conId)
        chain = next(c for c in chains if c.tradingClass == cnt.symbol and c.exchange == 'SMART')

        # separem strikes i expiracions (tenir en compte que strikes i expiracions estan en forma de Set, no de List
        lstrikes = chain.strikes

        # busquem el strike que més s'acosta a lastpricestk
        orderstrike = min(lstrikes, key=lambda x: abs(int(x) - lastpricestk))

        # busquem la expiration que més s'acosta a desiredexpiration
        lexps = []
        for e in chain.expirations: lexps.append(int(e))
        desiredexpiration = date.today() + timedelta(days=optdaystoexp)
        desiredexpiration = int(str(desiredexpiration)[0:4] + str(desiredexpiration)[5:7] + str(desiredexpiration)[8:10])
        orderexp = min(lexps, key=lambda x: abs(int(x) - desiredexpiration))

        # preparem el nou trade: definim i qualifiquem la nova opció
        optcnt = ibsync.Contract()
        optcnt.symbol = cnt.symbol
        optcnt.strike = orderstrike
        optcnt.secType = "OPT"
        optcnt.exchange = cf.myprefexchange
        optcnt.currency = cnt.currency
        optcnt.right = optright
        optcnt.lastTradeDateOrContractMonth = orderexp

        # no tots els strikes possibles (entre ells potser el ja triat) són vàlids.
        # si el strike triat no és vàlid en busquem un que sigui vàlid apujant (i baixant) el strike en 0.5
        # fins a trobar un que sigui acceptat. Això pot provocar que ens allunyem del ATM, però no hi ha altra solució
        ct = 0
        while ib.qualifyContracts(optcnt) == [] and ct < 11:
            orderstrike = orderstrike + 0.5*(optright == "C") - 0.5 * (optright == "P")
            optcnt.strike = int(orderstrike)
            ct += 1

        # busquem el preu al que cotitza la nova opció de la que obrirem contracte
        lastpriceopt = formatPrice(ib.reqTickers(optcnt)[0].marketPrice(), 2)

        # fem un reqN¡MktData per obtenir (hopefully) els Greeks
        opttkr = ib.reqMktData(optcnt, '', False, False)            # això torna un objecte Ticker
        l = 0
        while (opttkr.lastGreeks == None) and l < 5:  # mini-bucle per esperar que es rebin els Greeks
            opttkr = ib.reqMktData(optcnt, '', False, False)
            ib.sleep(5)
            l += 1
        # definim la quantitat = (Capital màxim)/(100*preu acció*Delta)
        # en cas que la delta torni buida, usem 0.5 (de moment agafem opcions AtTheMoney igualment)
        delta = 0.5
        if (opttkr.lastGreeks is not None):
            if (opttkr.lastGreeks.delta is not None):
                delta = opttkr.lastGreeks.delta
        qty = (1-2*(opttype == "SELL"))*round(cf.mymaxposition/(100*lastpricestk*abs(delta)))
        print("symbol  ", optcnt.symbol, "lastpricestk  ", lastpricestk, "desiredstrike", lastpricestk,
              "orderstrike  ", orderstrike, "desiredexpiration", desiredexpiration, "orderexp  ", orderexp,
              "quantity", qty, "conId", optcnt.conId, "lastpriceopt", lastpriceopt)
        if lastpriceopt == lastpriceopt:                            #checks if nan
            return tradelimitorder(ib, db, optcnt, qty, lastpriceopt, scode = scancode)
        else:
            return None
    except Exception as err:
        #error_handling(err)
        raise


# passem limit order. Torna una list amb les ordres llençades
# # scode = ScanCode i tType = TraeType són paràmetres opcionals per poder posar-los a la taula orders
def tradelimitorder(ib, db, contract, quantity, price, scode= None, ttype = None):
    try:
        ordertype  = "BUY" if quantity >= 0 else "SELL"
        order = ibsync.LimitOrder(ordertype, abs(quantity), price, tif="GTC", transmit=False)
        ib.qualifyContracts(contract)
        trade = ib.placeOrder(contract, order)
        ib.sleep(1)
        assert trade in ib.trades()
        assert order in ib.orders()
        # Insertem el contracte a la taule Contract (si no hi és)
        ibdb.dbfill_contracts(db, [contract])
        sql = "INSERT INTO orders (oOrderId, oClientId, oConId, oQuantity, oStatus, oScanCode, oTradeType) "\
            " VALUES (%s, %s, %s, %s, %s, %s, %s)"
        val = (order.orderId, order.clientId, trade.contract.conId, order.totalQuantity, trade.orderStatus.status, scode, ttype)
        execute_query(db, sql, val, True)
        return trade
    except Exception as err:
        #error_handling(err)
        raise


def openpositions(ib, db, accid):
    try:
        scansel = scanselection(db)
        scancode, scandesc = scansel[0], scansel[1]
        # getscannedstocks torna una llista de Contracts
        scannedstocklist = getscannedstocks(ib, scandesc)
        # fill fundamentals omple les llistes [contract(i), fundamentals(i)]
        scannedstocklist = fillfundamentals(ib, scannedstocklist)
        ibdb.dbfill_contractfundamentals(db, accid, scannedstocklist)
        return processpreselectedstocks(ib, db, accid, scannedstocklist, scancode)
    except Exception as err:
        #error_handling(err)
        raise


if __name__ == "__main__":
    try:
        myib = ibsync.IB()
        mydb = ibutil.dbconnect("localhost", "besuga", "xarnaus", "Besuga8888")
        acc = input("triar entre 'besugapaper', 'xavpaper', 'mavpaper1', 'mavpaper2'")
        if acc == "besugapaper":
            rslt = execute_query(mydb, "SELECT connHost, connPort, connAccId FROM connections WHERE connName = 'besugapaper7498'")
        elif acc == "xavpaper":
            rslt = execute_query(mydb, "SELECT connHost, connPort, connAccId FROM connections WHERE connName = 'xavpaper7497'")
        elif acc == "mavpaper1":
            rslt = execute_query(mydb, "SELECT connHost, connPort, connAccId FROM connections WHERE connName = 'mavpaper1'")
        elif acc == "mavpaper2":
            rslt = execute_query(mydb, "SELECT connHost, connPort, connAccId FROM connections WHERE connName = 'mavpaper2'")
        else:
            sys.exit("Unknown account!!")
        myib.connect(rslt[0][0], rslt[0][1], 1)
        myaccId = rslt[0][2]
        myorderdict = {}

        openpositions(myib, mydb, myaccId)


        ibutil.dbdisconnect(mydb)
        myib.disconnect()
    except Exception as err:
        error_handling(err)
        raise
