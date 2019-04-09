# importem llibreries
import sys
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
import bs4 as bs4
from bs4 import BeautifulSoup
import lxml
import lxml.etree
from numpy import sign
import re
from besuga_ib_utilities import *
#from besuga_ib_close_positions import tradelimitorder





def ib_connect(ip, port, clientid):
    # connectem a IBAPI
    myib.connect(ip, port, clientid)


def ib_disconnect():
    myib.disconnect()

def dbconnect(hname, dbname, uname, pwd):
    try:
        cnx = mysql.connector.connect(
            host=hname,
            database=dbname,
            user=uname,
            passwd=pwd
        )
    except mysql.connector.Error as err:
        if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
            print("Something is wrong with your user name or password")
        elif err.errno == errorcode.ER_BAD_DB_ERROR:
            print("Database does not exist")
        print(err)
        raise
    return cnx


def execute_query(mydb, query, values=None):
    try:
        mycursor = mydb.cursor()
        mycursor.execute(query, values)
        if (query.startswith('SELECT')):
            return mycursor.fetchall()
        elif (query.startswith('INSERT')):
            mydb.commit()
            return mycursor.lastrowid
        elif (query.startswith('ALTER')):
            mydb.commit()
            return mycursor.lastrowid
        elif (query.startswith('UPDATE') or query.startswith('DELETE')):
            mydb.commit()
            return mycursor.rowcount
    except Exception as err:
        raise




# passem limit order
def tradelimitorder(contract, quantity, ordertype, price, trId):
    #myib=IB()
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
    # print("ultimaordre   ",ultimaordre)
    # print("ordrespassadeslist   ",ordrespassadeslist)


def dbfill_contractfundamentals(db, stklst):
    try:
        sql = "INSERT INTO contractfundamentals (fConId, fType, fSymbol, fLocalSymbol, fCurrency, fExchange, fTradingClass, fRating, fTradeType, fScanCode,fEpsNext,fFrac52wk, fBeta, fPE0, fDebtEquity, fEVEbitda,fPricetoFCFShare, fYield,fROE, fTargetPrice, fConsRecom,fProjEPS, fProjEPSQ, fProjPE) "
        sql = sql + "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s)"
        for i in range(len(stklst)):
            check = execute_query(mydb, "SELECT fConId FROM contractfundamentals WHERE fConId = " + str(stklst[i][0]) )
            if (not check):
                print("fEpsNext  ",stklst[i][8])
                execute_query(db, sql, values = tuple(stklst[i]))
    except Exception as err:
        print(err)
        raise

def dbupdate_contractfundamentals(db, stklst):
    # stklst[i] conté [fConId, fType, fSymbol, fLocalSymbol, fCurrency, fExchange, fTradingClass, fRating, fTradeType, fScanCode,fEpsNext,fFrac52wk, fBeta, fPE0, fDebtEquity, fEVEbitda,fPricetoFCFShare, fYield,fROE, fTargetPrice, fConsRecom,fProjEPS, fProjEPSQ, fProjPE) "
    #         ]
    try:
        for i in range(len(stklst)):
            print('Updating contractfundamentals ' + str(stklst[i][0]))
            sql = "SELECT * FROM contractfundamentals WHERE fConId = '" + str(
                stklst[i][0]) + "' "
            rst=execute_query(db, sql)
            #print("rst   ",rst[0][2],rst[0][19])
            #print("stklsttttttttttt   ",stklst[i][19])


            if type(stklst[i][19])== "float"  and type(rst[0][19])=="float":
                stklst[i][6] = stklst[i][19] / rst[0][19]
            else:
                stklst[i][6]=0

            print("fSymbol  ",rst[0][2], "fTargetPrice  ",rst[0][19],"newtargetprice   ",stklst[i][19])
            sql = "UPDATE contractfundamentals SET frating = 0"
            execute_query(db, sql)
            '''
            if stklst[i][5] == 0:
                
            elif stklst[i][5] == 'M':
                sql = "UPDATE trades SET tShares = tShares - " + str(stklst[i][3]) + " ,tActive = 0 where tId = " + str(stklst[i][0])
                execute_query(db, sql)
                sql = "INSERT INTO trades (tExecid, tAccId, tConId, tTime, tShares, tPrice, tCommission, tLiquidation, "
                sql = sql + "toptPrice, toptIV, toptDelta, toptGamma, toptVega, toptTheta, toptPVDividend, toptPriceOfUnderlying, tActive) "
                # al nou registre, modifiquem l'Execid afegin-hi una C a davant, tActive=1 i tShares = stklst[i][5]
                new_execid = 'C' + stklst[i][1]
                sql = sql + "SELECT '" + new_execid + "',tAccId, tConId, tTime," + str(stklst[i][3]) + ", tPrice, tCommission, tLiquidation, "
                sql = sql + "toptPrice, toptIV, toptDelta, toptGamma, toptVega, toptTheta, "
                sql = sql + "toptPVDividend, toptPriceOfUnderlying, 1 "  # active = 1
                sql = sql + "FROM trades WHERE tId = " + str(stklst[i][0])
                execute_query(db, sql)
            '''

        selectedstocks =  pd.dataframe(stklst)
        print("selectedStocks", selectedstocks)
    except Exception as err:
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

def opennewposition(cnt,qty,optright):
    print("opennewposition")
    try:

        #instanciem objectes de la Class Contract
        stkcnt= Contract()
        optcnt=Contract()




        #qualifiquem el contracte del underlying (stock). de fet no fa falta, ja s'ha fet abans
        stkcnt.symbol = cnt.symbol
        stkcnt.currency = cnt.currency
        stkcnt.secType = cnt.secType
        stkcnt.exchange = cnt.exchange
        #stkcnt = cnt
        #print("stkcnt",stkcnt.symbol,stkcnt.currency,stkcnt.secType,stkcnt.exchange)
        print("stkcnt", stkcnt)
        #try:
        myib.qualifyContracts(stkcnt)
        #except:
        #    pass


        # agafem lastprice del underlying provinent de ticker
        tstk = myib.reqTickers(stkcnt)
        lastpricestk = tstk[0].marketPrice()
        myib.sleep(1)
        print("lastpricestk",lastpricestk)

        # busquem la cadena d'opcions del underlying
        chains = myib.reqSecDefOptParams(stkcnt.symbol, '', stkcnt.secType, stkcnt.conId)
        chain = next(c for c in chains if c.tradingClass == stkcnt.symbol and c.exchange == 'SMART')
        # print(util.df(chains))
        print("Chains  \n ", util.df(chains))



        # separem strikes i expiracions (tenir en compte que strikes i expiracions estan en forma de Set, no de List
        lexps = []
        lstrikes = []
        lexps2 = chain.expirations
        myib.sleep(1)
        lstrikes = chain.strikes
        myib.sleep(1)
        print("lexps   ",lexps,type(lexps))
        print("lstrikes   ", lstrikes,type(lstrikes))


        # busquem el strike que més s'acosta a desiredstrike
        desiredstrike = lastpricestk
        orderstrike = min(lstrikes, key=lambda x: abs(int(x)-lastpricestk))
        #orderstrike = min(lstrikes, key=lambda x: int(abs(int(x) - lastpricestk)))
        print("orderstrike", orderstrike)





        # busquem la expiration que més s'acosta a desiredexpiration
        lexps = []
        for e in lexps2:
            lexps.append(int(e))
        print(lexps)

        from datetime import date, datetime, timedelta
        desiredexpiration= date.today() + timedelta(days=daysnewpositions)
        d1=desiredexpiration
        desiredexpiration = int(str(d1)[0:4] + str(d1)[5:7] +  str(d1)[8:10])
        #print("typedesiredexpirtion",type(desiredexpiration))
        #desiredexpiration= datetime.strptime(desiredexpiration, "%Y,%m,%d")
        #print("today  ",datetime.now(), "desiredexpiration   ", desiredexpiration, type(int(desiredexpiration)))
        orderexp = min(lexps, key=lambda x: abs(int(x)-desiredexpiration))
        print ("orderexp",orderexp)



        # preparem el nou trade: definim i qualifiquem la nova opció
        optcnt.symbol = cnt.symbol
        optcnt.strike = orderstrike
        optcnt.secType = "OPT"
        optcnt.exchange = "SMART"
        optcnt.currency = cnt.currency
        optcnt.right = optright
        optcnt.lastTradeDateOrContractMonth = orderexp

        # no tots els strikes possibles (entre ells potser el ja triat) són vàlids.
        # si el strike triat no és vàlid en busquem un que sigui vàlid apujant (i baixant) el strike en 1
        #fins a trobar un que sigui acceptat. Això pot provocar que ens allunyem del ATM, però no hi ha altra solució
        sw1=0
        ct=0
        while sw1 == 0:
            qualify = myib.qualifyContracts(optcnt)
            #v2 = myib.reqContractDetails(optcnt)

            if qualify == [] and ct < 11:
                if optright == "C":
                    orderstrike = int(orderstrike + 0.5)
                    optcnt.strike = orderstrike
                    ct += 1
                elif optright == "P":
                    orderstrike = int(orderstrike - 0.5)
                    optcnt.strike = orderstrike
                    ct += 1
            else:
                sw1 = 1

        print("orderstrike1",orderstrike)



        '''
        print("optcnt",optcnt)
        #openint101 = myib.reqMktData(optcnt, '101', False, False)
        #openint22 = myib.reqMktData(optcnt, '22', False, False)
        #print("openint101 ",openint101)
        #print("openint22",openint22)
        #comprovem que la opció triada té openinterest
        
        opt = ib.reqMktData(optcnt, '100,101,105,106,107', False, False)
        
        #print("opt",opt)
        print("greeks",opt.lastGreeks.undPrice,opt.lastGreeks.delta)
        
        if (opt.lastGreeks is not None):
            lst2.append(opt.lastGreeks.optPrice)  # lst2[8]
            lst2.append(opt.lastGreeks.impliedVol)  # lst2[9]
            lst2.append(opt.lastGreeks.delta)  # lst2[10]
            lst2.append(opt.lastGreeks.gamma)  # lst2[11]
            lst2.append(opt.lastGreeks.vega)  # lst2[12]
            lst2.append(opt.lastGreeks.theta)  # lst2[13]
            lst2.append(opt.lastGreeks.pvDividend)  # lst2[14]
            lst2.append(opt.lastGreeks.undPrice)  # lst2[15]
        else:
            lst2.extend([0, 0, 0, 0, 0, 0, 0, 0])
        tickers = []
        for x in call_option_contracts:
            tickers.append(ib.reqTickers(x))
            print('Stock: %s| Strike: %d| Volume: %d| OpenInterest: %d' % (tickers[-1].contract.symbol,tickers[-1].contract.strike,tickers[-1].volume,tickers[-1].callOpenInterest))
        #use tickers array elsewhere
        print(tickers[0],tickers[10]) #just check        
        '''
        # busquem el preu al que cotitza la nova opció de la que obrirem contracte
        topt = myib.reqTickers(optcnt)
        lastpriceoptbis = (topt[0].bid + topt[0].ask) / 2
        # lastprice = formatPrice(lastprice, 2)

        lastpriceopt = topt[0].marketPrice()
        print("lastpriceopt  ", lastpriceopt, "lastpriceoptbis   ", lastpriceoptbis)
        myib.sleep(1)
        ordertype = ""

        # decidim si comprem o venem
        ordertype = 'SELL'

        print("optcnt", optcnt)
        print("symbol  ", optcnt.symbol, "lastpricestk  ", lastpricestk, "desiredstrike", desiredstrike,
              "orderstrike  ", orderstrike, "desiredexpiration", desiredexpiration, "orderexp  ", orderexp,
              "quantity",qty,"ordertype",ordertype,"conId",optcnt.conId,"price",lastpriceopt)

        #exit()

        # executem la ordre
        print("tradelimitorder  ", optcnt, abs(qty), ordertype, lastpriceopt, optcnt.conId)





        tradelimitorder(optcnt, abs(qty), ordertype, lastpriceopt, optcnt.conId)
    except Exception as err:
        print(err)
        #raise
        pass



def requestscanparameters():
    print("requestscanparameters")
    scanparams = myib.reqScannerParameters
    myib.sleep(5)
    print("scanaparams ", scanparams)
    # import xml.dom.minidom
    # xml = xml.dom.minidom.parse(a)  # or xml.dom.minidom.parseString(xml_string)
    # pretty_xml_as_string = xml.toprettyxml()
def scanstocks():
    print("scanstocks")

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
    # we define scancodes, for the time being only 52 week highs and lows

    scancodelist = ["HIGH_VS_52W_HL", "LOW_VS_52W_HL"]
    #scancodelist = ["HIGH_VS_52W_HL"]
    # we loop thru the scancodes selected and compose a list of stocks marking which scancode they proceed from
    stklst1 = []
    for sc in range(len(scancodelist)):
        vscancode= scancodelist[sc]
        scan_def = ScannerSubscription(instrument='STK',
                                   locationCode='STK.US.MAJOR',
                                   scanCode=vscancode,
                                   aboveVolume=200000,
                                   marketCapAbove=10000000000,
                                   averageOptionVolumeAbove=10000
                                   )
        scanner = myib.reqScannerData(scan_def, [])

        for stock in scanner[:10]:  # loops through stocks in the scanner
            rank = stock.rank
            contract = stock.contractDetails.contract
            secType = stock.contractDetails.contract.secType
            conId = stock.contractDetails.contract.conId
            symbol = stock.contractDetails.contract.symbol
            exchange = stock.contractDetails.contract.exchange
            currency = stock.contractDetails.contract.currency
            localSymbol = stock.contractDetails.contract.localSymbol
            tradingClass = stock.contractDetails.contract.tradingClass

            #cnt = Contract()
            #cnt.symbol = "IBM"
            #cnt.localSymbol = "IBM"
            #cnt.secType = "STK"
            #cnt.exchange = "SMART"
            #cnt.currency = "USD"

            cnt = stock.contractDetails.contract
            myib.qualifyContracts(cnt)
            '''
            cnt = Stock(stock.contractDetails.contract.symbol,
                        #stock.contractDetails.contract.conId,
                        stock.contractDetails.contract.exchange,
                        stock.contractDetails.contract.currency)

            '''
            #ticker = myib.reqTickers(cnt)

            stklst2 = []
            stklst2.append(vscancode)
            stklst2.append(symbol)
            stklst2.append(contract)


            stklst1.append(stklst2)




    print ("stklst1", stklst1)

    return stklst1



def fillfundamentalslist(stklst):
    print("fillfundamentalslist")
    try:
        stklst1 = []
        for s in range(len(stklst)):

            '''
            cnt = Contract()
            cnt.symbol = "IBM"
            cnt.localSymbol = "IBM"
            cnt.secType = "STK"
            cnt.exchange = "SMART"
            cnt.currency = "USD"
            myib.qualifyContracts(cnt)

            '''

            cnt = stklst[s][2]
            # print("cnt  ",cnt)


            fr = myib.reqMktData(cnt, "258")
            myib.sleep(10)
            print("fr", type(fr), cnt.symbol, fr)



            '''
            try:
                aux = dict(t.split('=') for t in str(fr.fundamentalRatios)[18:-1].split(',') if t)
                fratios = {key.lstrip(): value for key, value in aux.items()}
            except Exception as err:
                print(err)
                raise

            '''






            try:
                #fratios = dict(t.split('=') for t in str(fr.fundamentalRatios)[18:-1].split(',') if t)
                strfr = str(fr.fundamentalRatios)
                strfr = strfr.replace("FundamentalRatios(", "", 1)
                print("strfr", strfr)
                fratios = dict(t.split('=') for t in str(strfr).split(',') if t)



                print("fratios", fratios)
                fratios2 ={}
                for key in fratios.keys():
                    a = key[1:]
                    b = fratios[key]
                    fratios2[a]=b
                    print(a,b)
                #fratios = fratios2
                print("FRATIOS",fratios)
            except:
                continue




            if fratios != None:
                vbeta = fratios.get("BETA", "")
                vcurrency = fratios.get("CURRENCY", "")
                vfrac52wk = fratios.get("Frac52Wk", "")  # fraction of 52 week high/low
                vhigh52wk = fratios.get("NHIG", "")  # 52 week high
                vlow52wk = fratios.get("NLOW", "")  # 53 week low
                vapenorm = fratios.get("APENORM", "")  # annual normalized PE
                vpeexclxor = fratios.get("PEEXCLXOR", "")  # annual PE excluding extraordinary items
                vqtotd2eq = fratios.get("QTOTD2EQ", "")  # total debt/total equity
                vevcur = fratios.get("EV-Cur", "")  # Current enterprise value
                vevebitda = fratios.get("EV2EBITDA_Cur", "")  # Enterprise value/ebitda - TTM
                vprfcfshare = fratios.get("TTMPRFCFPS", "")  # price to free cash flow per share - TTM
                vyield = fratios.get("YIELD", "")  # Dividend yield
                vroepct = fratios.get("TTMROEPCT", "")  # return on equity %
                vepsnext = fratios.get("AFEEPSNTM", "")

                vbeta = fratios.get("BETA", "")
                vcurrency = fratios.get("CURRENCY", "")
                # vfrac52wk = fratios.get("Frac52Wk", "")  # fraction of 52 week high/low
                vhigh52wk = fratios.get("NHIG", "")  # 52 week high
                vlow52wk = fratios.get("NLOW", "")  # 53 week low
                vapenorm = fratios.get("APENORM", "")  # annual normalized PE
                vpeexclxor = fratios.get("PEEXCLXOR", "")  # annual PE excluding extraordinary items
                vqtotd2eq = fratios.get("QTOTD2EQ", "")  # total debt/total equity
                vevcur = fratios.get("EV-Cur", "")  # Current enterprise value
                vevebitda = fratios.get("EV2EBITDA_Cur", "")  # Enterprise value/ebitda - TTM
                vprfcfshare = fratios.get("TTMPRFCFPS", "")  # price to free cash flow per share - TTM
                vyield = fratios.get("YIELD", "")  # Dividend yield
                vroepct = fratios.get("TTMROEPCT", "")  # return on equity %
                print("vBeta  ",vbeta,"vepsnext  ",vepsnext, "vFrac52Wk  ",vfrac52wk)



                vepsnext = vepsnext.split(".")
                vfrac52wk = vfrac52wk.split(".")
                vbeta = vbeta.split(".")
                vapenorm = vapenorm.split(".")
                vqtotd2eq = vqtotd2eq.split(".")
                vevebitda = vevebitda.split(".")
                vprfcfshare = vprfcfshare.split(".")
                vyield = vyield.split(".")
                vroepct = vroepct.split(".")

                if len(vepsnext) == 1:
                    vepsnext.append(0)
                if len(vfrac52wk) == 1:
                    vfrac52wk.append(0)
                if len(vbeta) == 1:
                    vbeta.append(0)
                if len(vapenorm) == 1:
                    vapenorm.append(0)
                if len(vqtotd2eq) == 1:
                    vqtotd2eq.append(0)
                if len(vevebitda) == 1:
                    vevebitda.append(0)
                if len(vprfcfshare) == 1:
                    vprfcfshare.append(0)
                if len(vyield) == 1:
                    vyield.append(0)
                if len(vroepct) == 1:
                    vroepct.append(0)



                #print("type of vepsnext list components   ",type(vepsnext),type(vepsnext[0]),type(vepsnext[1]),vepsnext,vepsnext[0],vepsnext[1])


                vepsnext[1] = "0." + str(vepsnext[1])
                if vepsnext[0]=="":
                    vepsnext[0]=0
                vepsnext = float(vepsnext[0]) + float(vepsnext[1])
                vepsnext = round(vepsnext, 2)

                vfrac52wk[1] = "0." + str(vfrac52wk[1])
                if vfrac52wk[0]=="":
                    vfrac52wk[0]=0
                vfrac52wk = float(vfrac52wk[0]) + float(vfrac52wk[1])
                vfrac52wk = round(vfrac52wk, 2)
                
                vbeta[1] = "0." + str(vbeta[1])
                if vbeta[0]=="":
                    vbeta[0]=0
                vbeta = float(vbeta[0]) + float(vbeta[1])
                vbeta = round(vbeta, 2)
                
                vapenorm[1] = "0." + str(vapenorm[1])
                if vapenorm[0]=="":
                    vapenorm[0]=0
                vapenorm = float(vapenorm[0]) + float(vapenorm[1])
                vapenorm = round(vapenorm, 2)
                
                vqtotd2eq[1] = "0." + str(vqtotd2eq[1])
                if vqtotd2eq[0]=="":
                    vqtotd2eq[0]=0
                vqtotd2eq = float(vqtotd2eq[0]) + float(vqtotd2eq[1])
                vqtotd2eq = round(vqtotd2eq, 2)
                
                vevebitda[1] = "0." + str(vevebitda[1])
                if vevebitda[0]=="":
                    vevebitda[0]=0
                vevebitda = float(vevebitda[0]) + float(vevebitda[1])
                vevebitda = round(vevebitda, 2)
                
                vprfcfshare[1] = "0." + str(vprfcfshare[1])
                if vprfcfshare[0]=="":
                    vprfcfshare[0]=0
                vprfcfshare = float(vprfcfshare[0]) + float(vprfcfshare[1])
                vprfcfshare = round(vprfcfshare, 2)
                
                vyield[1] = "0." + str(vyield[1])
                if vyield[0]=="":
                    vyield[0]=0
                vyield = float(vyield[0]) + float(vyield[1])
                vyield = round(vyield, 2)
                
                vroepct[1] = "0." + str(vroepct[1])
                if vroepct[0]=="":
                    vroepct[0]=0
                vroepct = float(vroepct[0]) + float(vroepct[1])
                vroepct = round(vroepct, 2)


                print("VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVV",vepsnext)


                addfunds = requestadditionalfundamentals(cnt)
                print("addfunds", addfunds, type(addfunds))
                # we fill the list with fundamental data that we will use to update database + make computations to select
                # candidates to open positions
                # a vegades requestadditionalfundamentals toran buit, per això el "if df not
                if addfunds != None:
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
                    stklst2.append(stklst[s][0])

                    '''
                    vepsnext = float(vepsnext.strip())
                    vfrac52wk = vfrac52wk.strip()
                    vbeta = vbeta.strip()
                    vapenorm = vapenorm.strip()
                    vqtotd2eq = vqtotd2eq.strip()
                    vevebitda = vevebitda.strip()
                    vprfcfshare = float(vprfcfshare.strip())
                    vyield = vyield.strip()
                    vroepct = vroepct.strip()
                    '''

                    stklst2.append(vepsnext)
                    stklst2.append(vfrac52wk)
                    stklst2.append(vbeta)
                    stklst2.append(vapenorm)
                    stklst2.append(vqtotd2eq)
                    stklst2.append(vevebitda)
                    stklst2.append(vprfcfshare)
                    stklst2.append(vyield)
                    stklst2.append(vroepct)

                    stklst2.append(addfunds["TargetPrice"])
                    stklst2.append(addfunds["ConsRecom"])
                    stklst2.append(addfunds["ProjEPS"])
                    stklst2.append(addfunds["ProjEPSQ"])
                    stklst2.append(addfunds["ProjPE"])

                    stklst1.append(stklst2)

                    print("stklst2  ", stklst2)
        dbfill_contractfundamentals(mydb, stklst1)
        df = pd.DataFrame(stklst1)
        print("stklst1  ", stklst1)
        print("df   ", df)

        return (stklst1)

    except Exception as err:
        print(err)
        raise


def requestadditionalfundamentals(cnt):
    print("requestFundamentalData")

    '''
    cnt = Contract()
    cnt.symbol = contract.symbol
    cnt.localSymbol = contract.localSymbol
    cnt.secType = contract.secType
    cnt.exchange = contract.exchange
    cnt.primaryExchange = contract.primary
    cnt.currency = contract.currency
    myib.qualifyContracts(cnt)    
    '''

    fundamentals = myib.reqFundamentalData(cnt, 'ReportSnapshot')
    print("fundamentals   ",cnt, fundamentals)
    #soup = BeautifulSoup(fundamentals, 'lxml')
    #print("soup  ", soup)
    #print("pretiffy      ",soup.prettify())



    if fundamentals != []:
        import xmltodict
        print("fundamentals",cnt, fundamentals)
        #with open('path/to/file.xml') as fd:
        doc = xmltodict.parse(fundamentals)
        print("Type doc", type(doc))

        myib.sleep(2)
        print(doc)



        kw = doc
        dictfilt = lambda x, y: dict([(i, x[i]) for i in x if i in set(y)])
        wanted_keys = ("TargetPrice", "ProjEPS")
        result = dictfilt(kw, wanted_keys)
        print("RESULT   ",result)

        #a = [x for x in doc.items]

        dictratios ={}
        print("KEYS                     ")
        print(doc['ReportSnapshot'].keys())
        print("ITEMS                    ")
        print(doc.items())
        print(list(doc.keys())[0][3])
        print("RATIOS")
        print(doc['ReportSnapshot']['Ratios'])
        print("FORECASTDATA")
        print(doc['ReportSnapshot']['ForecastData'])
        print("FORECASTDATA-RATIO")
        print(doc['ReportSnapshot']['ForecastData']['Ratio'])
        print("FORECASTDATA-RATIO-5")
        print(doc['ReportSnapshot']['ForecastData']['Ratio'][5])


        #print("len ReportSnapshot Ratios Group")
        #print("len  ", len(doc['ReportSnapshot']['Ratios']['Group']))
        for i in range(len(doc['ReportSnapshot']['Ratios']['Group'])):
            print(doc['ReportSnapshot']['Ratios']['Group'][i])
            #print(doc['ReportSnapshot']['Ratios']['Ratio'][i]['@FieldName'])
            #print(doc['ReportSnapshot']['Ratios']['Ratio'][i]['Value']['#text'])

        #print("len ReportSnapshot ForecastData Ratio")
        #print("len  ", len(doc['ReportSnapshot']['ForecastData']['Ratio']))
        for i in range(len(doc['ReportSnapshot']['ForecastData']['Ratio'])):
            print(doc['ReportSnapshot']['ForecastData']['Ratio'][i])
            dkey=(doc['ReportSnapshot']['ForecastData']['Ratio'][i]['@FieldName'])
            dvalue=(doc['ReportSnapshot']['ForecastData']['Ratio'][i]['Value']['#text'])
            dvalue = dvalue.split(".")
            if len(dvalue) == 1:
                dvalue.append(0)
            dvalue[1]= "0."+str(dvalue[1])
            dvalue = float(dvalue[0]) + float(dvalue[1])
            dvalue = round(dvalue, 2)
            dictratios[dkey]=dvalue
        print("dictratios   ",dictratios)
        return(dictratios)
        #ordered_dict[ordered_dict.keys()[index]]

        #doc[fundamentals]['@has'] # == u'an attribute'
        #doc['mydocument']['and']['many']  # == [u'elements', u'more elements']
        #doc['mydocument']['plus']['@a']  # == u'complex'
        #doc['mydocument']['plus']['#text']  # == u'element as well'

        #import xml.dom.minidom
        #vxml = xml.dom.minidom.parse(fundamentals)  # or xml.dom.minidom.parseString(xml_string)
        #print("vxml   ",vxml)
        #pretty_xml_as_string = xml.toprettyxml()

        # The fundamentals are returned as xml so make a beautifulsoup object
        #from bs4 import BeautifulSoup
        #a = bs4.builder.builder_registry.builders
        #print ("bs4builder  ",a)




    '''

    # Parse the xml for the fields you want    
    CoID = soup.find('CoID', Type='CompanyName')
    gross_margin = soup.find('Ratio', FieldName='TTMGROSMGN').string
    ebitda = soup.find('Ratio', FieldName='TTMEBITD').string
    targetPrice = soup.find('Ratio', FieldName='TargetPrice').string
    projPE = soup.find('Ratio', FieldName='ProjPE').string
    print(CoID.string, gross_margin, ebitda, targetPrice, projPE)
    '''






    '''
    a= myib.reqFundamentalData("IBM", 'ReportSnapshot')
    print ("reqfundamentaldata",a)

    # loop through the scanner results and get the contract details
    for stock in scan_results:
        security = Stock(stock.contractDetails.contract.symbol,
                         stock.contractDetails.contract.exchange,
                         stock.contractDetails.contract.currency)

        # request the fundamentals
        a = ib.reqFundamentalData(security, 'ReportSnapshot')

        print(a)


        # The fundamentals are returned as xml so make a beautifulsoup object
        soup = BeautifulSoup(fundamentals, 'xml')
        # Parse the xml for the fields you want
        CoID = soup.find('CoID', Type='CompanyName')
        gross_margin = soup.find('Ratio', FieldName='TTMGROSMGN').string
        ebitda = soup.find('Ratio', FieldName='TTMEBITD').string
        print(CoID.string, gross_margin, ebitda)

        '''

    #raise SystemExit

def processpreselectedstocks(stklst):
    print("processpreselectedstocks")

    try:

        # definim namedtuple "prestocks per a processar candidats a noves posicions
        prestocks = namedtuple("prestocks", "ntConId ntType ntSymbol ntLocalSymbol ntCurrency ntExchange \
                                             ntTradingClass ntRating ntTradeType ntScanCode ntEpsNext ntFrac52wk ntBeta ntPE0  \
                                             ntDebtEquity ntEVEbitda ntPricetoFCFshare ntYield ntROE ntTargetPrice ntConsRecom ntProjEPS ntProjEPSQ ntProjPE ")



        # passem les execucions obertes en forma de namedtuple a la llista "opentrades"
        listprestocks = []
        for i in range(len(stklst)):
            pstk = prestocks(ntConId=stklst[i][0], ntType=stklst[i][1], ntSymbol =stklst[i][2], ntLocalSymbol=stklst[i][3], ntCurrency=stklst[i][4],
                           ntExchange=stklst[i][5], ntTradingClass=stklst[i][6], ntRating=stklst[i][7],ntTradeType=stklst[i][8],ntScanCode=stklst[i][9], ntEpsNext=stklst[i][10],
                           ntFrac52wk=stklst[i][11], ntBeta=stklst[i][12], ntPE0=stklst[i][13], ntDebtEquity=stklst[i][14],
                           ntEVEbitda=stklst[i][15], ntPricetoFCFshare=stklst[i][16], ntYield =stklst[i][17], ntROE=stklst[i][18], ntTargetPrice=stklst[i][19], ntConsRecom=stklst[i][20],
                           ntProjEPS = stklst[i][21], ntProjEPSQ=stklst[i][22], ntProjPE=stklst[i][23])
            listprestocks.append(pstk)

            #print("targetprice   ",pstk.ntTargetPrice, "rating   ", pstk.ntRating)


        dfps = pd.DataFrame(listprestocks, columns=prestocks._fields)

        #print("dfps  ",dfps)
        for index, row in dfps.iterrows():

            sql = "SELECT * FROM contractfundamentals WHERE fConId = '" + str(
                row['ntConId'] ) + "' "
            rst = execute_query(mydb, sql)
            #print("rst   ",rst[0][2],rst[0][19],row['ntTargetPrice'])
            #print("typesssss",type(row['ntTargetPrice']),type(rst[0][19]))
            #if type(row['ntTargetPrice']) == "float" and type(rst[0][19]) == "float":

            if row['ntScanCode'] == 'HIGH_VS_52W_HL':
                print("HIGHHHHH",row['ntSymbol'],row['ntTargetPrice'], rst[0][19])
                if dfps.loc[index, 'ntTargetPrice'] > rst[0][19]:
                    dfps.loc[index, 'ntRating'] = 1

            if row['ntScanCode'] == 'LOW_VS_52W_HL':
                print("LOWWWW",row['ntSymbol'], row['ntTargetPrice'], rst[0][19])
                if dfps.loc[index, 'ntTargetPrice'] < rst[0][19]:
                    dfps.loc[index, 'ntRating'] = 2

            #sql = "UPDATE contractfundamentals SET fTargetPrice = '" + str(a) + "'"
            #sql = "UPDATE contractfundamentals SET fTargetPrice = '" + a + "' WHERE fConId = '" + row['ntConId'] + "'"
            #sql = "UPDATE contractfundamentals SET fTargetPrice = '" + a + "'" # WHERE fConId = '" + row['ntConId'] + "'"
            #sql = "UPDATE contractfundamentals SET fTargetPrice =" + a # WHERE fConId = '" + row['ntConId'] + "'"
            sql = "UPDATE contractfundamentals SET fTargetPrice = " + str(row['ntTargetPrice']) + "WHERE fConId = " + str(row['ntConId'])
            execute_query(mydb, sql)
            #print("dfps.index",dfps.index)


        print(dfps[['ntSymbol','ntScanCode','ntRating','ntTargetPrice']])
        #print(dfps['ntSymbol','ntScanCode','ntRating','ntTargetPrice'])



        #exit()





        #dfps = dfps.reset_index(drop=True)
        #dfps_slice = dfps.ix[0:]
        #dfps_slice.reset_index()
        dfps.reset_index()
        for index, row in dfps.iterrows():
            print("rating",row['ntSymbol'],row['ntRating'])

            if row['ntRating'] == 1:
                qty=1
                right = "P"
                cnt = Contract()
                cnt.symbol = row['ntSymbol']
                cnt.localSymbol = row['ntLocalSymbol']
                cnt.secType = row['ntType']
                cnt.exchange = row['ntExchange']
                cnt.currency = row['ntCurrency']
                #print("contract to qualify", cnt, row['ntType'])
                try:
                    myib.qualifyContracts(cnt)
                except:
                    pass
                print("opennewposition", cnt, cnt.symbol, qty, right)
                opennewposition(cnt, qty, right)
            elif row['ntRating'] == 2:
                qty = 1
                right = "C"
                cnt = Contract()
                cnt.symbol = row['ntSymbol']
                cnt.localSymbol = row['ntLocalSymbol']
                cnt.secType = row['ntType']
                cnt.exchange = row['ntExchange']
                cnt.currency = row['ntCurrency']
                #print("contract to qualify", cnt)
                try:
                    myib.qualifyContracts(cnt)
                except:
                    pass
                #print("contract to qualify", cnt)
                print("opennewposition", cnt, cnt.symbol, qty, right)
                opennewposition(cnt, qty, right)
            else:
                pass

            #if row['ntScanCode'] == 'HIGH_VS_52W_HL':
            #    right = "P"
            #if row['ntScanCode'] == 'LOW_VS_52W_HL':
            #    right = "C"



        #exit()


        '''
            if type(stklst[i][19]) == "float" and type(rst[0][19]) == "float":
                stklst[i][6] = stklst[i][19] / rst[0][19]
            else:
                stklst[i][6] = 0

            print("fSymbol  ", rst[0][2], "fTargetPrice  ", rst[0][19], "newtargetprice   ", stklst[i][19])
            sql = "UPDATE contractfundamentals SET frating = 0"
            execute_query(mydb, sql)





            print(row['ntSymbol'], row['ntRating'],row['ntScanCode'],row['ntProjPE'])
            print("nrRating before  ",row['ntRating'])
            if row['ntScanCode'] == "HIGH_VS_52W_HL" and row['ntProjPE'] < 15:
                row['ntRating']=1
                print(row['ntSymbol'], row['ntRating'],row['ntScanCode'],row['ntProjPE'])

        
        for prestock in listprestocks:
            if prestock.ntScanCode == "HIGH_VS_52W_HL" and prestock.ntProjPE < 15:

                 print(prestock)


        
        '''

        print("listprestocks  ", listprestocks)
        #dbupdate_contractfundamentals(mydb, listprestocks)
    except Exception as err:
        print(err)
        raise




if __name__ == "__main__":
    # MAIN BODY




    # inicialització paràmeteres
    global ordrespassadeslist
    ordrespassadeslist = []
    daysnewpositions = 45 # distancia en dies per noves posicions, comptant desde dia present
    print(sys.version)

    myib = IB()
    mydb = dbconnect("localhost", "besuga", "mav", "BESUGA8888")
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
















    #mydb = dbconnect2(dbs)
    #connectIBAPI()
    #rslt = execute_query(mydb, "SELECT connHost, connPort FROM connections WHERE connName = 'besugapaper7498'")
    #rslt = execute_query(mydb, "SELECT connHost, connPort FROM connections WHERE connName = 'mavpapermasacc'")




    #requestscanparameters()

    '''
    #experiment amb un sol contracte
    cnt = Contract()
    cnt.symbol = "IBM"
    cnt.localSymbol = "IBM"
    cnt.secType = "STK"
    cnt.exchange = "SMART"
    cnt.currency = "USD"
    # cnt = stock.contractDetails.contract
    myib.qualifyContracts(cnt)
    opennewposition(cnt, 1, "C")
    exit()
    '''



    scannedstocklist = scanstocks()
    scannedstocklist = fillfundamentalslist(scannedstocklist)
    print("scannedstocklist", scannedstocklist)
    processpreselectedstocks(scannedstocklist)










    '''
    fdict = requestAdditionalFundamentals(cnt)
    print("fdict   ", fdict)
    print("Targetprice   ",type(fdict["TargetPrice"]))
    '''

    #a= fundamentalData(IBM)
    #print("fundamentalData   ",a )
    #a= myib.reqMarketDataType(4)
    #print("A  ", a)
    # analitzem Accoount
    #accountAnalysis()

    # analitzem posicions obertes
    # portfolio_analysis()

    # introduïm trades a db
    # get_trades()





    # desconnectem de IBAPI
    ib_disconnect()

