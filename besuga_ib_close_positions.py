# Standard library imports
import sys
from datetime import datetime

# Third party imports
import ib_insync as ibsync

# Local application imports
from besuga_ib_utilities import error_handling
from besuga_ib_utilities import execute_query
from besuga_ib_utilities import diffdays
from besuga_ib_utilities import formatPrice
from besuga_ib_open_positions import tradelimitorder
import besuga_ib_manage_db_positions as ibdb
import besuga_ib_utilities as ibutil
import ib_config as cf


def opendefensiveoption(ib, db, opt, pos, tradetype):
    try:
        print("opendefensiveoption")
        # underlying de la opció
        stkcnt = ibsync.contract.Stock(symbol = opt.symbol, exchange = cf.myprefexchange, currency = opt.currency)
        ib.qualifyContracts(stkcnt)
        # busquem la cadena d'opcions del underlying
        chains = ib.reqSecDefOptParams(stkcnt.symbol, '', stkcnt.secType, stkcnt.conId)
        chain = next(c for c in chains if c.tradingClass == stkcnt.symbol and c.exchange == cf.myprefexchange)
        # agafem lastprice del underlying provinent de ticker
        lastpricestk = ib.reqTickers(stkcnt)[0].marketPrice()
        ib.sleep(1)

        # Construïm la opció defensiva
        defoptstrike = min(chain.strikes, key=lambda x: int(abs(int(x) - lastpricestk)))
        defoptright = "C" if opt.right == "P" else "P"
        defopt = ibsync.contract.Option(symbol = opt.symbol, lastTradeDateOrContractMonth = opt.lastTradeDateOrContractMonth, \
                                        strike = defoptstrike, right = defoptright, exchange = cf.myprefexchange, \
                                        multiplier = opt.multiplier, currency = opt.currency)
        ib.qualifyContracts(defopt)
        # no tots els strikes possibles (entre ells potser el ja triat) són vàlids.
        # si el strike triat no és vàlid en busquem un que sigui vàlid apujant (i baixant) el strike en 0.5
        # fins a trobar un que sigui acceptat. Això pot provocar que ens allunyem del ATM, però no hi ha altra solució
        ct = 0
        while ib.qualifyContracts(defopt) == [] and ct < 11:
            defopt.strike = orderstrike = int(defoptstrike + 0.5 * (defoptright == "C") - 0.5 * (defoptright == "P"))
            ct += 1
        pricedefopt = formatPrice(ib.reqTickers(defopt)[0].marketPrice(), 2)

        print("opendefensiveposition - ", defopt, "position: ", pos, "price: ", pricedefopt)
        tradelimitorder(ib, db, defopt, pos, pricedefopt, ttype = tradetype)
    except Exception as err:
        #error_handling(err)
        raise

# Mirem en quines condicions hem de tancar o obrir opcions defensives
def allowTrade(dateearnings, pcttimeelapsed, pctprofitnow, sectype, shortposition):
    try:
        allowtrade = None
        # si la data de Earnings està aprop, tanquem posició regardless
        if diffdays(dateearnings, datetime.now().strftime("%Y%m%d")) <= cf.mydaystoearnings:
            allowtrade = 'EARNDATE'
        elif sectype == "OPT":
            if pctprofitnow >= cf.myoptprofit: allowtrade = 'OPTW'
            elif pcttimeelapsed <= 10 and pctprofitnow > cf.myoptprofit10: allowtrade = 'OPTW-10'
            elif pcttimeelapsed <= 20 and pctprofitnow > cf.myoptprofit20: allowtrade = 'OPTW-20'
            elif pcttimeelapsed <= 50 and pctprofitnow > cf.myoptprofit50: allowtrade = 'OPTW-50'
            elif pcttimeelapsed <= 75 and pctprofitnow > cf.myoptprofit75: allowtrade = 'OPTW-75'
            if pctprofitnow <= cf.myoptloss: allowtrade = 'OPTL'
            #elif pctprofitnow <= cf.myoptlossdef and shortposition: allowtrade = 'OPTL -D'
        elif sectype == "STK":
            if pctprofitnow >= cf.mystkprofit: allowtrade = 'STK1'
            elif pctprofitnow <= cf.mystkloss: allowtrade = 'STK2'
        else:
            print("I don't know how to handle thie security type: ", sectype)
        return allowtrade
    except Exception as err:
        #error_handling(err)
        raise


def processopenpositions(ib, db):
    print("\nprocessopenpositions")
    try:
        # llegim posicions obertes de IB: llista d'objectes (namedtuples) PortfolioItem (atributs: contract, position, marketPrice, marketValue, averageCost, unrealizedPNL, realizedPNL, account)
        for pos in ib.portfolio():
            pctprofitnow = 100*pos.unrealizedPNL/(pos.averageCost*abs(pos.position))             # percentatge de guanys/pèrdues
            dateearnings = ibdb.getearningsdate(db, pos.contract.conId)
            # TEMPORAL DEGUT A INCONSISTENCIES A LA BBDD
            #if dateearnings == None: dateearnings = datetime.now().strftime("%Y%m%d")
            print(pos.contract.localSymbol, "\t pctprofitnow: ", round(pctprofitnow, 2), "\t Earnings Date: ", dateearnings)
            pcttimeelapsed = 0                                                                   # inicialitzem
            # calculem el percentatge de temps transcorregut des de l'obertura respecte el temps fins a expiració
            if pos.contract.secType == 'OPT':
                dateentry = ibdb.get_positiondate(db, pos.account, pos.contract.conId)
                datetoday = datetime.now().strftime("%Y%m%d")
                dateexpiration = pos.contract.lastTradeDateOrContractMonth
                #TEMPORAL DEGUT A INCONSISTENCIES A LA BBDD
                #if dateentry == None or datetoday == None or dateexpiration == None:
                #    pcttimeelapsed = 100
                #else:
                pcttimeelapsed = 100*diffdays(dateentry, datetoday) / diffdays(dateentry, dateexpiration)
            # comprobem si s'ha de fer alguna cosa
            allowtrade = allowTrade(dateearnings, pcttimeelapsed, pctprofitnow, pos.contract.secType, pos.position < 0)
            if allowtrade != None:
                fmtprice = formatPrice(pos.marketPrice, 2)
                if allowtrade == 'OPTL -D' and pos.position < 0:
                    opendefensiveoption(ib, db, pos.contract, pos.position, allowtrade)
                else:
                    tradelimitorder(ib, db, pos.contract, -pos.position, fmtprice, ttype = allowtrade + str(cf.myoptloss))              #-posició per tancar el què tenim
                    print("Close Position ", pos, "\n\t due to: ", allowtrade, "\t price: ", fmtprice)
        print ("\n Closing Positions analysis finalised \n")
    except Exception as err:
        #error_handling(err)
        raise


if __name__ == "__main__":
    try:
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
        processopenpositions(myib, mydb)


        ibutil.dbdisconnect(mydb)
        # desconnectem de IBAPI
        myib.disconnect()
    except Exception as err:
        error_handling(err)
        raise


