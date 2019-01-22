


def ibConnect(ip, port, clientid):
    # connectem a IBAPI
    myib.connect(ip, port, clientid)


def ibDisconnect():
    myib.disconnect()


def connectIBAPI():
    wb = op.load_workbook("BesugaIBAPI.xlsx")
    sheet = wb.get_sheet_by_name("Gontech")
    ip = sheet["B2"].value
    port = sheet["B3"].value
    clientid = sheet["B4"].value
    # print (ip,port,clientid)
    myib.connect(ip, port, clientid)


def dbconnectOld():
    try:
        cnx = mysql.connector.connect(
            host='localhost',
            user='mav',
            passwd='BESUGA8888',
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


def dbconnect(dbs):
    try:
        cnx = mysql.connector.connect(
            host='localhost',
            user='mav',
            passwd='BESUGA8888',
            database=dbs
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


def error_handling(e, initial_text='Exception'):
    # mostra un pop-up d'error
    root = tk.Tk()
    root.withdraw()
    text = initial_text + 'Error Number({0}): {1}'.format(e.errno, e.strerror)
    msgbox.showerror("Error", text)


def dbfill_trades(db, trades):
    # print("def dbfill_trades")
    sql = "SET FOREIGN_KEY_CHECKS=0"
    execute_query(db, sql)
    sql = "INSERT INTO trades (tExecid, tAccId, tConId, tTime, tShares, tPrice, tCommission, tLiquidation, toptprice, toptIV, toptDelta, toptGamma, toptVega, toptTheta, toptPVDividend,toptPriceOfUnderlying,tActive) "
    sql = sql + "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s,%s,%s)"
    for i in range(len(trades)):
        try:
            execute_query(db, sql, tuple(trades[i]))
        except Exception as err:
            if err.errno == errorcode.ER_DUP_ENTRY:
                continue
            else:
                print(err)
                raise
    sql = "SET FOREIGN_KEY_CHECKS=1"
    execute_query(db, sql)


def dbupdate_trades(db, trId):
    print("dbupdate_trades")
    print("update trades")

    sql = "SET FOREIGN_KEY_CHECKS=0"
    execute_query(db, sql)

    # sql = "UPDATE trades tActive = 0 WHERE tId = trId) "
    # sql = "UPDATE trades tActive = 0 WHERE tId = trId) "
    # execute_query(db, sql)
    sql = "SET FOREIGN_KEY_CHECKS=1"
    execute_query(db, sql)


#   inserts contracts when contract not in table mysql.contract
#   it can only be an 'INSERT' statement since a contract never changes
def dbfill_contracts(db, contr):
    try:
        sql = "INSERT INTO contracts (kConId, kType, kSymbol, kLocalSymbol, kCurrency, kExchange, kTradingClass, kExpiry, kStrike, kRight, kMultiplier) "
        sql = sql + "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s ,%s)"
        for i in range(len(contr)):
            check = execute_query(mydb, "SELECT kConId FROM contracts WHERE kConId = " + str(contr[i][0]))
            if (not check):
                execute_query(db, sql, tuple(contr[i]))
    except Exception as err:
        print(err)
        raise


def portfolio_to_dict(ib_):
    try:
        pfl = ib_.portfolio()
        # dictionary de lists que contindrà les dades que volem recuperar de l'objecte Contract per cada PortfolioItem del Portfolio
        d_contr = {'secType': [], 'conId': [], 'symbol': [], 'exchange': [], 'primaryExchange': [], 'currency': [],
                   'localSymbol': []}
        # OrderedDict de lists que contindrà les dades que volem recuperar de la namedtupla PortfolioItem (excepte els detalls del Contract) per cada PortfolioItem del Portfolio del Portfolio
        d_pfl = {'position': [], 'marketPrice': [], 'marketValue': [], 'averageCost': [], 'unrealizedPNL': [],
                 'realizedPNL': [], 'account': []}
        # recorrem tots els PortfoioItema Portfolio
        for i in range(len(pfl)):
            ib_.qualifyContracts(pfl[i].contract)
            for k in d_contr.keys():
                # afegim els valors (cada value de (key,value) és una llista) de cada atribut que recuperem de l'objecte Contract d'aquest PortfolioItem.
                d_contr[k].append(getattr(pfl[i].contract, k))
                print("getatr   ", getattr(pfl[i].contract, k))
            for k in d_pfl.keys():
                # afegim els valors (cada value de (key,value) és una llista) de cada valor que m'interessa de Portfolio Item ( a part dels detalls del contracte, recuperats abans)
                d_pfl[k].append(getattr(pfl[i], k))
        # posem tota la informació al dictionary pfl_values
        d_pfl.update(d_contr)
        # ordenem i retornem un OrderedDict
        my_order = ['conId', 'symbol', 'localSymbol', 'currency', 'secType', 'position', 'averageCost', 'marketPrice',
                    'marketValue', 'unrealizedPNL', 'realizedPNL']
        od_pfl = OrderedDict((k, d_pfl[k]) for k in my_order)
        return od_pfl
    except Exception as e:
        msg = 'Exception in function portfolio_to_dict \n'
        error_handling(e, msg)
        raise


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
def tradeLimitOrder(contract, quantity, ordertype, price, trId):
    global ordresPassadesList
    order = LimitOrder(ordertype, quantity, price, tif="GTC", transmit=False)
    myib.qualifyContracts(contract)
    trade = myib.placeOrder(contract, order)
    myib.sleep(1)
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


def opendefensiveposition(cnt):
    try:
        print("opendefensiveposition")
        # creem objectes tupus contracte
        stkcnt = Contract()  # el underlying de la opció
        optcnt1 = Contract()  # la opció que hi ha al portfolio
        optcnt2 = Contract()  # la potencial nova opció que es crearà


        # composem el contracte del underlying de la opció analitzada
        stkcnt = Stock(cnt.symbol, "SMART", cnt.currency)
        myib.qualifyContracts(stkcnt)


        # composem el contracte de la opció analitzada
        dateexpiration = str(cnt.lastTradeDateOrContractMonth)[0:4] + str(cnt.lastTradeDateOrContractMonth)[4:6] + str(cnt.lastTradeDateOrContractMonth)[6:8]
        optcnt1.symbol = cnt.symbol
        optcnt1.localSymbol = cnt.localSymbol
        optcnt1.secType = cnt.secType
        optcnt1.exchange = "SMART"
        optcnt1.currency = cnt.currency
        myib.qualifyContracts(optcnt1)

        # agafem lastprice del underlying provinent de ticker
        tstk = myib.reqTickers(stkcnt)
        topt1 = myib.reqTickers(optcnt1)
        lastpricestk = tstk[0].marketPrice()
        lastpriceopt1 = topt1[0].marketPrice()
        myib.sleep(1)

        # busquem la cadena d'opcions del underlying
        chains = myib.reqSecDefOptParams(stkcnt.symbol, '', stkcnt.secType, stkcnt.conId)
        chain = next(c for c in chains if c.tradingClass == stkcnt.symbol and c.exchange == 'SMART')
        print("chain  ", chain)
        # print(util.df(chains))

        # separem strikes i expiracions
        lexps = []
        lstrikes = []
        lexps = chain.expirations
        lstrikes = chain.strikes
        myList = lstrikes
        lastpricestk = int(lastpricestk)
        # calculem la distància entre el preu del underlying ara i el strike de la opció venuda que estem analitzant
        strikedistance = abs(cnt.strike - lastpricestk)
        # busquem l'strike que més s'acosta al del preu actual del underlying
        orderstrike = min(lstrikes, key=lambda x: int(abs(int(x) - lastpricestk)))
        print("strikedistance", strikedistance, "lastpricestk  ", lastpricestk, "orderstrike  ", orderstrike)

        # preparem el nou trade: opció compemsatoria....si era un call ara un put...i al inreves
        if cnt.right == "C":
            opt2right = "P"
        else:
            opt2right = "C"

        # preparem el nou trade: qualifiquem la nova opció compensatoria
        optcnt2.symbol = cnt.symbol
        optcnt2.strike = orderstrike
        optcnt2.secType = cnt.secType
        optcnt2.exchange = "SMART"
        optcnt2.currency = cnt.currency
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
        if cnt.shares < 0:
            ordertype = 'SELL'
        else:
            ordertype = 'BUY'
        # executem la ordre
        print("tradelimitorder  ", optcnt2, abs(cnt.shares), ordertype, lastpriceopt2, cnt.conId)
        tradeLimitOrder(optcnt2, abs(cnt.shares), ordertype, lastpriceopt2, optcnt2.conId)
        # tradeLimitOrder(cnt, abs(qty), orderType, abs(fmtprice), cnt.conId)
    except Exception as err:
        print(err)
        raise


def opendefensivepositionOld(cnt,trd):
    try:
        print("opendefensiveposition")
        # creem objectes tupus contracte
        stkcnt = Contract()  # el underlying de la opció
        optcnt1 = Contract()  # la opció que hi ha al portfolio
        optcnt2 = Contract()  # la potencial nova opció que es crearà

        # composem el contracte del underlying de la opció analitzada
        #stkcnt = Stock(cnt.symbol, "SMART", cnt.currency)
        stkcnt = cnt
        myib.qualifyContracts(stkcnt)

        # composem el contracte de la opció analitzada


        #optcnt1.symbol = trd.symbol
        #optcnt1.localSymbol = trd.localSymbol
        #optcnt1.secType = trd.type
        #optcnt1.exchange = "SMART"
        #optcnt1.currency = trd.currency

        optcnt1.conId = trd.conId
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

        # print(util.df(chains))

        # separem strikes i expiracions
        lexps = []
        lstrikes = []
        lexps = chain.expirations
        lstrikes = chain.strikes
        myList = lstrikes
        lastpricestk = int(lastpricestk)
        # calculem la distància entre el preu del underlying ara i el strike de la opció venuda que estem analitzant
        strikedistance = abs(trd.strike - lastpricestk)
        # busquem l'strike que més s'acosta al del preu actual del underlying
        orderstrike = min(lstrikes, key=lambda x: int(abs(int(x) - lastpricestk)))
        print("strikedistance", strikedistance, "lastpricestk  ", lastpricestk, "orderstrike  ", orderstrike)

        # preparem el nou trade: opció compemsatoria....si era un call ara un put...i al inreves
        if trd.right == "C":
            opt2right = "P"
        else:
            opt2right = "C"

        # preparem el nou trade: qualifiquem la nova opció compensatoria
        optcnt2.symbol = optcnt1.symbol
        optcnt2.strike = orderstrike
        optcnt2.secType = optcnt1.sectype
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
        if trd.shares < 0:
            ordertype = 'SELL'
        else:
            ordertype = 'BUY'
        # executem la ordre
        print("tradelimitorder  ", optcnt2, abs(trd.shares), ordertype, lastpriceopt2, trd.conId)
        tradeLimitOrder(optcnt2, abs(trd.shares), ordertype, lastpriceopt2, optcnt2.conId)
        # tradeLimitOrder(cnt, abs(qty), orderType, abs(fmtprice), trd.conId)
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
        if pctprofitnow >= 0:
            allowtrade = 2
    elif sectype =="STK":
        if pctprofitnow >= 20:
            allowtrade = 3
        if pctprofitnow <= -20:
            allowtrade = 4
    else:
        allowtrade = 0
    return allowtrade


def processtrades():
    print("processtrades")
    try:
        # llegim trades oberts de la base de dades


        query = "SELECT tId, tExecId, tAccId, tConid, tTime, tShares, tPrice, tCommission, tLiquidation,toptPrice, toptIV, toptDelta," \
                " toptGamma, toptVega, toptTheta, toptPVDividend, toptPriceOfUnderlying, tActive" \
                " FROM trades LEFT JOIN contracts ON trades.tConId = contracts.kConId" \
                " WHERE tAccId =  '" + vAccId + "' AND tActive = 1"


        rst = execute_query(mydb, query, values=None)
        # definim namedtuple "trades" per a processar execucions obertes
        trades = namedtuple("trades", "Id execId accId conId  \
                                     time shares price commission liquidation optPrice optIV \
                                     optDelta optGamma optVega optTheta optPVDividend \
                                     optPriceOfUnderlying active")
        # passem les execucions obertes en forma de namedtuple a la llista "opentrades"
        opentrades = []
        for i in range(len(rst)):
            trade = trades(Id=rst[i][0], execId=rst[i][1], accId=rst[i][2], conId=rst[i][3],
                           time=rst[i][4], shares=rst[i][5], price=rst[i][6], commission=rst[i][7],
                           liquidation=rst[i][8], optPrice=rst[i][9], optIV=rst[i][10],
                           optDelta=rst[i][11], optGamma=rst[i][12], optVega=rst[i][13],
                           optTheta=rst[i][14], optPVDividend=rst[i][15],
                           optPriceOfUnderlying=rst[i][16], active=rst[i][17])
            opentrades.append(trade)

        # llegim "opentrades" en forma de loop per a decidir què fer amb cada execució oberta


        ordresPassadesList = []
        pctProfitList = []
        for trd in opentrades:
            #creem un objecte Contract
            cnt = Contract()
            #fem una instancia de contract amb el contracte llegit del query de trades oberts de la db trades
            cnt.conId = trd.conId

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
                if trd.conId == f.contract.conId:
                    lastprice = f.marketPrice
                    #lastprice = f.marketValue
            # demanem dades a traves de reqMktData
            # m_data = myib.reqMktData(cnt)
            # while m_data.last != m_data.last: myib.sleep(0.01)  # Wait until data is in.
            # myib.cancelMktData(cnt)
            # print("m_data   ",m_data.last)

            avgcost = float(trd.price)
            vshares = trd.shares
            # calculem pctprofitnow (el pnl de la posició)
            if vshares < 0:
                pctprofitnow = (1 - (lastprice / avgcost)) * 100
            else:
                pctprofitnow = ((lastprice / avgcost) - 1) * 100
            print(cnt.symbol,"  ",vshares, "lastprice   ",lastprice,"avgcost",avgcost,"pctprofitnow  ",pctprofitnow)

            # calculem percentatge temps passat entre apertura posició i expiració per a posicions d'opcions
            pctpostimeelapsed = 0
            if cnt.secType == "OPT": #and trd.shares < 0:
                dateentry = str(trd.time)[0:4] + str(trd.time)[5:7] + str(trd.time)[8:10]
                datetoday = datetime.datetime.now().strftime("%Y%m%d")
                datedifffromentry = diffdays(dateentry, dateexpiration)
                datedifffromtoday = diffdays(datetoday, dateexpiration)
                pctpostimeelapsed = int((1 - datedifffromtoday / datedifffromentry) * 100)



            # d'acord amb els paràmetres calculats decidim si es fa un trade o no a la funció "allowtrade"
            #allowtrade = allowTrade(pctpostimeelapsed, pctprofitnow)
            allowtrade  = allowTrade(pctpostimeelapsed, pctprofitnow,cnt.secType)
            #allowtrade = 0
            pctProfitList.append(
                [cnt.symbol, trd.shares, cnt.right, cnt.strike, trd.price, lastprice, int(pctprofitnow),
                 pctpostimeelapsed, allowtrade])

            # allowtrade = 1 tancar posició per recollida de beneficis, allowtrade = 2 fem un trade defensio de la posició
            if allowtrade == 1:
                if trd.shares < 0:
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
                tradeLimitOrder(cnt, abs(vshares), ordertype, abs(fmtprice), trd.conId)
            elif allowtrade == 2:
                # SELECCIONEM CONTRACTES PER TRADE DEFENSIU
                opendefensivepositionOld(cnt,trd)
                pass

            elif allowtrade == 3:
                if trd.shares < 0:
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
                tradeLimitOrder(cnt, abs(vshares), ordertype, abs(fmtprice), trd.conId)
            elif allowtrade == 4:
                if trd.shares < 0:
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
                tradeLimitOrder(cnt, abs(vshares), ordertype, abs(fmtprice), trd.conId)
            elif allowtrade == "8888":
                # PLACE  MAEKET ORDER
                # MarketOrder(cnt, abs(vshares), ordertype, abs(fmtprice), trd.conId)
                #Order = MarketOrder(ordertype,abs(vshares))
                #trade = myib.placeOrder(cnt,Order)
                pass
            else:
                pass

        print("pctProfitList             ", pd.DataFrame(pctProfitList), "sheet1")
        print("ordresPassadesList", ordresPassadesList)
    except Exception as err:
        print(err)
        raise


def dbanalyze_executions(db):
    sql = "SELECT DISTINCT(tConId), COUNT(*) FROM activetrades GROUP BY tConId  HAVING COUNT(tConId)>1 ORDER BY kSymbol, tTime"
    try:
        lst = execute_query(db, sql)  # distinct executions for the same contract + number of executions
        final_list = []
        for i in range(len(lst)):
            sql = "SELECT tId, tExecId, tConId, tShares, tPrice, tActive FROM activetrades WHERE tConId = " + str(
                lst[i][0]) + " "
            sql = sql + "ORDER BY SIGN(tShares), tTime"
            execs = execute_query(db, sql)
            k = 0
            aux = 0
            # trobar l'índex(k)a partir del qual els valors són positius
            for j in range(1, len(execs)):
                if (sign(execs[j][3]) != sign(execs[j - 1][3])): k = j
            for j in range(0, k):
                if k <= len(execs):
                    if abs(execs[j][3]) < abs(execs[k][3]):  # Comparació de les +/- shares
                        execs[j] = list(execs[j])  # transfrormem la tupla en una llista
                        execs[j][5] = 0  # posarem el registre a tActive = 0
                        aux = execs[j][3] + execs[k][3]
                    elif abs(execs[j][3]) == abs(execs[k][3]):
                        execs[j] = list(execs[j])
                        execs[k] = list(execs[k])
                        execs[j][5] = 0
                        execs[k][5] = 0
                        aux = 0
                        k += 1
                    else:
                        execs[k] = list(execs[k])
                        execs[k][5] = 0  # posarem el registre a tActive = 0
                        aux = execs[j][3] + execs[k][3]
                        k += 1
                last = [j, k]
            if (aux > 0):  # mirem si hi ha més compres que vendes
                execs[last[1]] = list(execs[last[1]])
                execs[last[1]][5] = aux  # marquem el saldo positiu que queda
            elif (aux < 0):
                execs[last[0]] = list(execs[last[0]])
                execs[last[0]][5] = aux  # marquem el saldo negatiu que queda
            for j in range(0, len(execs)):
                final_list.append(execs[j])
        return final_list
    except Exception as err:
        print(err)
        raise


def dbupdate_executions(db, execs):
    # execs[i] conté [tId, tExecId, tConId, tShares, tPrice, tActive]
    try:
        print('EXECS ', execs)
        for i in range(len(execs)):
            if execs[i][5] == 0:
                sql = "UPDATE trades SET tActive = 0 where tId = " + str(execs[i][0])
                execute_query(db, sql)
            elif execs[i][5] != 1:
                sql = "UPDATE trades SET tActive = 0 where tId = " + str(execs[i][0])
                execute_query(db, sql)
                sql = "INSERT INTO trades (tExecid, tAccId, tConId, tTime, tShares, tPrice, tCommission, tLiquidation, "
                sql = sql + "toptPrice, toptIV, toptDelta, toptGamma, toptVega, toptTheta, toptPVDividend, toptPriceOfUnderlying, tActive) "
                # al nou registre, modifiquem l'Execid afegin-hi una C a davant, tActive=1 i tShares = execs[i][5]
                new_execid = 'C' + execs[i][1]
                sql = sql + "SELECT '" + new_execid + "',tAccId, tConId, tTime," + str(
                    execs[i][5]) + ", tPrice, tCommission, tLiquidation, "
                sql = sql + "toptPrice, toptIV, toptDelta, toptGamma, toptVega, toptTheta, "
                sql = sql + "toptPVDividend, toptPriceOfUnderlying, 1 "  # active = 1
                sql = sql + "FROM trades WHERE tId = " + str(execs[i][0])
                execute_query(db, sql)
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
    pctprofittarget = -3

    # creem instancia de IB
    myib = IB()



    #  creem instancia de connexió db al mateix temps que triem compte a IB amb el que operem

    acc = input("triar entre 'besugapaper' o 'mavpaper1' o 'mavpaper2'")
    if acc == "besugapaper":
        dbs = "besuga"
        mydb = dbconnect(dbs)
        rslt = execute_query(mydb,
                             "SELECT connHost, connPort, connAccId FROM connections WHERE connName = 'besugapaper7498'")
    elif acc == "mavpaper1":
        dbs = "besuga"
        mydb = dbconnect(dbs)
        rslt = execute_query(mydb, "SELECT connHost, connPort, connAccId FROM connections WHERE connName = 'mavpaper1'")
    elif acc == "mavpaper2":
        dbs = "besuga"
        mydb = dbconnect(dbs)
        rslt = execute_query(mydb, "SELECT connHost, connPort, connAccId FROM connections WHERE connName = 'mavpaper2'")


    # connectem a IB
    myib.connect(rslt[0][0], rslt[0][1], 1)

    # Inicialitzem variable que conté Id compte a IB
    vAccId = ""
    vAccId = rslt[0][2]




    #demanem delayed data
    myib.reqMarketDataType(4)
    #analitzem Accoount
    accountAnalysis()

    # analitzem posicions obertes
    #portfolio_analysis()

    # introduïm trades a db
    dbfill_trades(mydb, get_trades(myib))
    dbfill_contracts(mydb, get_contracts(myib))

    # processem trades oberts, tancant els que calgui i obrint trades defensius si cal
    processtrades()

    aux = dbanalyze_executions(mydb)
    print('AUX ', aux)
    dbupdate_executions(mydb, aux)

    # selectoptioncontract()

    # desconnectem de IBAPI
    ibDisconnect()

