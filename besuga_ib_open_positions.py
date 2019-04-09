# Standard library imports 
import sys
from datetime import date, timedelta

# Third party imports
import ib_insync as ibsync
import xmltodict

# Local application imports
from besuga_ib_utilities import error_handling
from besuga_ib_utilities import execute_query
from besuga_ib_utilities import tradelimitorder
import besuga_ib_utilities as ibutil
import ib_insync as ibconfig


# scannerparms = [instrument, locationCode, scanCode, aboveVolume, marketCapAbove, averageOptionVolumeAbove]
# maxstocke = màximum number of stocke returned by the scan
def scanstocks(ib, scan, maxstocks):
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
        print("\n\t scanstocks ")
        stklst = []
        scanner = ib.reqScannerData(scan, [])
        for stock in scanner[:maxstocks]:  # loops through stocks in the scanner
            contr = stock.contractDetails.contract
            ib.qualifyContracts(contr)
            stk = []
            stk.append(scan.scanCode)           #scancode
            stk.append(contr)                   #contract
            print("scanstocks :: stock :: ", stk)
            stklst.append(stk)
        return stklst
    except Exception as err:
        error_handling(err)
        raise


def fillfundamentals(ib, stklst):
    print("\n\t fillfundamentals")
    try:
        for i in range(len(stklst)):
            scancode = stklst[i][0]             #scan code
            cnt = stklst[i][1]                  #contract
            fr = ib.reqMktData(cnt, "258")
            ib.sleep(10)
            aux = dict(t.split('=') for t in str(fr.fundamentalRatios)[18:-1].split(',') if t)
            fratios = {key.lstrip(): value for key, value in aux.items()}
            addfunds = requestadditionalfundamentals(ib, cnt)
            # we fill the list with fundamental data that we will use to update database + make computations to select
            # candidates to open positions
            # a vegades requestadditionalfundamentals toran buit, per això el "if df not
            stklst2 = []
            stklst2.append(cnt.conId)  # lst2[0]
            stklst2.append(cnt.secType)  # lst2[1]
            stklst2.append(cnt.symbol)  # lst2[2]
            stklst2.append(cnt.localSymbol)  # lst2[3]
            stklst2.append(cnt.currency)  # lst2[4]
            stklst2.append(cnt.exchange)  # lst2[5]
            stklst2.append(cnt.tradingClass)
            stklst2.append(0)
            stklst2.append(0)
            stklst2.append(scancode)

            if fratios != None:
                stklst2.append(fratios.get("AFEEPSNTM", ""))
                stklst2.append(fratios.get("Frac52Wk", ""))             # fraction of 52 week high/low
                stklst2.append(fratios.get("BETA", ""))
                stklst2.append(fratios.get("APENORM", ""))              # annual normalized PE
                stklst2.append(fratios.get("QTOTD2EQ", ""))             # total debt/total equity
                stklst2.append(fratios.get("EV2EBITDA_Cur", ""))        # Enterprise value/ebitda - TTM
                stklst2.append(fratios.get("TTMPRFCFPS", ""))           # price to free cash flow per share - TTM
                stklst2.append(fratios.get("YIELD", ""))                # Dividend yield
                stklst2.append(fratios.get("TTMROEPCT", ""))            # return on equity %
            else:
                stklst2.extend([0, 0, 0, 0, 0, 0, 0, 0, 0])

                '''
                Not used attributes????
                vcurrency = fratios.get("CURRENCY", "")
                vhigh52wk = fratios.get("NHIG", "")  # 52 week high
                vlow52wk = fratios.get("NLOW", "")  # 53 week low
                vpeexclxor = fratios.get("PEEXCLXOR", "")  # annual PE excluding extraordinary items
                vevcur = fratios.get("EV-Cur", "")  # Current enterprise value
                '''

            if addfunds != None:
                stklst2.append(addfunds["TargetPrice"])
                stklst2.append(addfunds["ConsRecom"])
                stklst2.append(addfunds["ProjEPS"])
                stklst2.append(addfunds["ProjEPSQ"])
                stklst2.append(addfunds["ProjPE"])
            else:
                stklst2.extend([0, 0, 0, 0, 0])

            for j in range(len(stklst2)):
                if stklst2[j] == '': stklst2[j] = 0
                if stklst2[j] == 'nan': stklst2[j] = 0
                if stklst2[j] is None: stklst2[j] = 0

            stklst[i] = stklst2
            print("stklst2  ", stklst2)
        return (stklst)
    except Exception as err:
        error_handling(err)
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
        error_handling(err)
        raise

def processpreselectedstocks(ib, db, accid, stklst):
    print("\n\t processpreselectedstocks")
    try:
        listorders = []
        qty = -1
        for i in range(len(stklst)):
            cnt = ibsync.Contract()
            cnt.conId = stklst[i][0]
            ib.qualifyContracts(cnt)
            sql = "SELECT * FROM contractfundamentals WHERE fConId = '" + str(cnt.conId) + "' " \
                  + " AND fAccId = '" + accid + "' "
            rst = execute_query(db, sql)
            # si scancode = HIGH_VS_52W_HL i TargetPrice > el que està guardat a la base de dades
            print("stklst[", i, "] ", stklst[i], " \n stklst[i][19] ", stklst[i][19], " \n rst[0][19] ", rst[0][19])
            if stklst[i][9] == 'HIGH_VS_52W_HL' and stklst[i][19] > rst[0][19]:
                print("Open new LOW_VS_52W_HL -  Put ", cnt.symbol)
                listorders.append(opennewoption(ib, cnt, qty, "P", ibconfig.myoptdaystoexp))
            elif stklst[i][9] == 'LOW_VS_52W_HL' and stklst[i][19] < rst[0][19]:
                print("Open new LOW_VS_52W_HL -  Call ", cnt.symbol)
                listorders.append(opennewoption(ib, cnt, qty, "C", ibconfig.myoptdaystoexp))
            elif stklst[i][9] == 'HOT_BY_VOLUME':
                print("ProcessPreselectedStocks HOT_BY_VOLUME ")
            else:
                print("I’m sorry Besuga, I’m afraid I can’t do that:             ", stklst[i][9]," ", cnt.symbol, "Target Price ", stklst[i][19])
            # actualitzem els fundamentals a la base de dades
            dbupdate_contractfundamentals(db, accid, stklst[i])
        return listorders
    except Exception as err:
        error_handling(err)
        raise


def dbfill_contractfundamentalsNEW(db, accid, stklst):
    try:
        sql = "INSERT INTO contracts (kConId, kType, kSymbol, kLocalSymbol, kCurrency, kExchange, kTradingClass, kExpiry, kStrike, kRight, kMultiplier) "
        sql = sql + "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s ,%s)"
        for i in range(len(stklst)):
            check = execute_query(db, "SELECT * FROM contracts WHERE kConId = " + str(contr[i][0]))
            if (not check):
                execute_query(db, sql, values=tuple(contr[i]), commit=True)
        sql = "INSERT INTO contractfundamentals (fAccId, fConId, fType, fSymbol, fLocalSymbol, fCurrency, fExchange, fTradingClass, fRating, fTradeType, fScanCode,fEpsNext,fFrac52wk, fBeta, fPE0, fDebtEquity, fEVEbitda,fPricetoFCFShare, fYield,fROE, fTargetPrice, fConsRecom,fProjEPS, fProjEPSQ, fProjPE) "
        sql = sql + "VALUES (str(accid), %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)"
        for i in range(len(stklst)):
            check = execute_query(db, "SELECT fConId FROM contractfundamentals WHERE fConId = " + str(stklst[i][0]) )
            if (not check):
                execute_query(db, sql, values = tuple(stklst[i]), commit = True)
    except Exception as err:
        error_handling(err)
        raise


def dbfill_contractfundamentals(db, accid, stklst):
    try:
        print(accid)
        sql = "INSERT INTO contractfundamentals (fAccId, fConId, fType, fSymbol, fLocalSymbol, fCurrency, fExchange, fTradingClass, fRating, fTradeType, fScanCode,fEpsNext,fFrac52wk, fBeta, fPE0, fDebtEquity, fEVEbitda,fPricetoFCFShare, fYield,fROE, fTargetPrice, fConsRecom,fProjEPS, fProjEPSQ, fProjPE) "
        sql = sql + "VALUES ('"+ accid + "', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)"
        for i in range(len(stklst)):
            check = execute_query(db, "SELECT fConId FROM contractfundamentals WHERE fConId = " + str(stklst[i][0]) )
            if (not check):
                execute_query(db, sql, values = tuple(stklst[i]), commit = True)
    except Exception as err:
        error_handling(err)
        raise


def dbupdate_contractfundamentals(db, accid, instrument):
    try:
        sql = "UPDATE contractfundamentals set fEpsNext = %s, fFrac52wk = %s, fBeta = %s, fPE0 = %s, fDebtEquity = %s, fEVEbitda = %s,fPricetoFCFShare = %s, " \
                "fYield = %s, fROE = %s, fTargetPrice = %s, fConsRecom = %s,fProjEPS = %s, fProjEPSQ = %s, fProjPE = %s " \
                " WHERE fConId = " + str(instrument[0]) + " AND fAccId = '" + accid + "' "
        val = tuple(instrument[10:])
        print ("sql ", sql, "\nvalues ", val)
        execute_query(db, sql, values = val, commit = True)
    except Exception as err:
        error_handling(err)
        raise


def opennewoption(ib, cnt, qty, optright, optdaystoexp):
    print("\n\t opennewoption")
    try:
        # agafem lastprice del underlying provinent de ticker
        lastpricestk = ib.reqTickers(cnt)[0].marketPrice()
        # busquem la cadena d'opcions del underlying
        chains = ib.reqSecDefOptParams(cnt.symbol, '', cnt.secType, cnt.conId)
        chain = next(c for c in chains if c.tradingClass == cnt.symbol and c.exchange == 'SMART')

        # separem strikes i expiracions (tenir en compte que strikes i expiracions estan en forma de Set, no de List
        lstrikes = []
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
        optcnt.exchange = "SMART"
        optcnt.currency = cnt.currency
        optcnt.right = optright
        optcnt.lastTradeDateOrContractMonth = orderexp

        # no tots els strikes possibles (entre ells potser el ja triat) són vàlids.
        # si el strike triat no és vàlid en busquem un que sigui vàlid apujant (i baixant) el strike en 0.5
        # fins a trobar un que sigui acceptat. Això pot provocar que ens allunyem del ATM, però no hi ha altra solució
        ct = 0
        while ib.qualifyContracts(optcnt) == [] and ct < 11:
            optcnt.strike = orderstrike = int(orderstrike + 0.5*(optright == "C") - 0.5 * (optright == "P") )
            ct += 1
        # busquem el preu al que cotitza la nova opció de la que obrirem contracte
        topt = ib.reqTickers(optcnt)
        lastpriceopt = topt[0].marketPrice()

        print("symbol  ", optcnt.symbol, "lastpricestk  ", lastpricestk, "desiredstrike", lastpricestk,
              "orderstrike  ", orderstrike, "desiredexpiration", desiredexpiration, "orderexp  ", orderexp,
              "quantity", qty, "conId", optcnt.conId, "price", lastpriceopt)

        if lastpriceopt == lastpriceopt:                            #checks if nan
            return tradelimitorder(ib, optcnt, qty, lastpriceopt)
        else:
            return None
    except Exception as err:
        error_handling(err)
        raise


def openpositions(ib, db, accid, scan, maxstocks):
    try:
        scannedstocklist = scanstocks(ib, scan, maxstocks)
        scannedstocklist = fillfundamentals(ib, scannedstocklist)
        dbfill_contractfundamentals(db, accid, scannedstocklist)
        return processpreselectedstocks(ib, db, accid, scannedstocklist)
    except Exception as err:
        error_handling(err)
        raise


if __name__ == "__main__":

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
    myordersdict = {}

    for i in range(len(ibconfig.myscancodelist)):
        scan = ibsync.ScannerSubscription(instrument='STK', locationCode='STK.US.MAJOR', scanCode=ibconfig.myscancodelist[i],
                                         aboveVolume=200000, marketCapAbove=10000000000, averageOptionVolumeAbove=10000)
        myordersdict[ibconfig.myscancodelist[i]] = openpositions (myib, mydb, myaccId, scan,ibconfig.mymaxstocks)

    ibutil.dbdisconnect(mydb)
    myib.disconnect()

