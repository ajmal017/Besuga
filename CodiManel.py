import datetime
import statistics
import sys

import besuga_ib_config as cf
# Local application imports
import besuga_ib_utilities as ibutil
import besuga_ib_manage_db as ibdb
# Third party imports
import ib_insync as ibsync
from besuga_ib_manage_db import dbfill_accounthistory
from besuga_ib_utilities import error_handling
from besuga_ib_utilities import execute_query
from besuga_ib_utilities import formatPrice
from besuga_ib_open_positions import opennewoption


import bs4 as bs
#import pickle
import requests
import statistics as stats



def save_tickers(source):
    resp = requests.get(source)
    soup = bs.BeautifulSoup(resp.text, 'lxml')
    table = soup.find('table', {'class': 'wikitable sortable'})
    table = table.findAll('tr')
    tickers = []
    for row in table[1:]:
        ticker = row.findAll('td')[0].text
        ticker= ticker.rstrip("\n")
        tickers.append(ticker)
    return tickers


# prdnum = número de periodes (1,2,10,35, etc)
# prdscale = unitat en la que es mesuren els periodes:(S(segons),D (dia), W(semana), M(mes), Y(any)
# barsize: duració temporal de cada "bar": "1 secs", "5 secs", "10 secs", "15 secs", "30 secs"
# "1 min", "2 mins", "3 mins", "5 mins", "10 mins", "15 mins", "20 mins", "30 mins"
# "1 hour", "2 hours", "4 hours", "8 hours"
# "1 day", "1 week", "1 month"
# time periods allowed: S(seconds), D(days),W(weeks),M(months),Y(year)
# tickerId: A unique identifier which will serve to identify the incoming data
# endDatetime: the request's end date and time (the empty string indicates current present moment)
def requesthistoricaldata(cnt,prdnum,prdscale,barsize):
    try:
        bars = myib.reqHistoricalData(cnt, endDateTime='', durationStr=str(prdnum)+ " " + prdscale,
                                    barSizeSetting=barsize, whatToShow="MIDPOINT", useRTH=True)
        # fem una llista amb els CLOSE per a cada unitat del periode que analitzem (menys el 'dia' d'avui)
        listbars = [bars[i].close for i in range(len(bars)-1)]

        #calculem el màxim i el múnim del periode per a poder determinar puts de breakout (al alça i a la baixa)
        maxbars = max(listbars, key=lambda x: x)
        minbars = min(listbars, key=lambda x: x)

        # també calculem la desviació estandard, perquè és cool i queda molt quant
        stdbars = round(stats.stdev(listbars),2)

        # busquem el preu al que cotitza el contracte
        lastpricecnt = formatPrice(myib.reqTickers(cnt)[0].marketPrice(), 2)

        if lastpricecnt > maxbars:
            print(cnt.symbol," ----------- nou màxim","maxbars", maxbars, "minbars", minbars, "lastpricecnt", lastpricecnt, "stdbars", stdbars)
            return 1
        elif lastpricecnt < minbars:
            print(cnt.symbol," ----------- nou minim","maxbars", maxbars, "minbars", minbars, "lastpricecnt", lastpricecnt, "stdbars", stdbars)
            return -1
        else:
            print(cnt.symbol, "res a fer","maxbars", maxbars, "minbars", minbars, "lastpricecnt", lastpricecnt, "stdbars", stdbars)
            return 0

    except Exception as e:
        # error_handling(e)
        raise


if __name__ == '__main__':
    try:
        myib = ibsync.IB()
        mydb = ibutil.dbconnect(cf.dbhost, cf.dbname, cf.dbuser, cf.dbpassword)
        rslt = []
        q = input("Triar entre 'besugapaper', 'xavpaper', 'mavpaper1', 'mavpaper2' - exit to quit ")
        while q != "exit":
            sql = "SELECT connHost, connPort, connAccId, connClientId FROM connections WHERE "
            if q.lower() == "besugapaper":
                rslt = ibutil.execute_query(mydb, sql + " connName = 'besugapaper7498'")
                break
            elif q.lower() == "xavpaper":
                rslt = ibutil.execute_query(mydb, sql + " connName = 'xavpaper7497'")
                break
            elif q.lower() == "mavpaper1":
                rslt = ibutil.execute_query(mydb, sql + " connName = 'mavpaper1'")
                break
            elif q.lower() == "mavpaper2":
                rslt = ibutil.execute_query(mydb, sql + " connName = 'mavpaper2'")
                break
            elif q.lower() == "exit":
                sys.exit("Exit requested! ")
            else:
                q = input ("Unknown account! ")

        myib.connect(rslt[0][0], rslt[0][1], rslt[0][3])
        myaccId = rslt[0][2]

        tickers= save_tickers('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
        # tickers= save_tickers('https: // en.wikipedia.org / wiki / NASDAQ - 100  # Components')
        # tickers= save_tickers('https://www.nasdaq.com/quotes/nasdaq-100-stocks.aspx')
        for ticker in tickers:
            cnt = ibsync.Contract(symbol = ticker, secType = "STK", currency = "USD", exchange = "SMART")
            isopen = ibdb.positionisopen(mydb, myaccId, ticker )
            if not myib.qualifyContracts(cnt) != [] and not isopen:
                try:
                    breakout = requesthistoricaldata(cnt, 5, "Y", "1 week")
                except ValueError as e:
                    print(e, cnt)
                    continue
                if  breakout > 0:
                    opennewoption(myib, mydb, cnt, "SELL", "P", cf.myoptselldte, "5YHIGH")
                elif breakout < 0:
                    opennewoption(myib, mydb, cnt, "SELL", "C", cf.myoptselldte, "5YLOW")

    except Exception as err:
        #error_handling(err)
        raise