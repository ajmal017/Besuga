# importem llibreria insync i numpy i panda i tkinter
import numpy as np
import pandas as pd
import tkinter as tk
from tkinter import messagebox as msgbox
from collections import OrderedDict
import openpyxl as oppy
from ib_insync import *

# definim funcions

def ibConnect(ip,port,clientid):
    ib.connect(ip, port, clientid)


def ibDisconnect():
    ib.disconnect()

#def error(self, reqId: TickerId, errorCode: int, errorString: str):
#    super().error(reqId, errorCode, errorString)
#    print("Error. Id: ", reqId, " Code: ", errorCode, " Msg: ", errorString)


def accountAnalysis():
    accSum = ib.accountSummary()
    accVal = ib.accountValues()
    for p in accSum:
        print(' ',p.tag,'   ', p.value,'\n')

    #print(reqAccountSummaryTags.AllTags)
def tradeLimitOrder(contract,quantity,ordertype,price):

    print("contract: ",contract)
    print("ordertype ", ordertype)
    print("quantity: ", quantity)
    print("price: ", price)

    #order = LimitOrder(ordertype, quantity, price)
    order = LimitOrder(ordertype, quantity, price, tif="GTC",transmit=False)

    ib.qualifyContracts(contract)
    trade = ib.placeOrder(contract, order)
    print('TRADE: ', trade)
    ib.sleep(1)
    print('TRADE LOG: ', trade.log)

def computePrice(price):
    precision = 3
    #floats = [1.123123123, 2.321321321321]
    newPrice = np.round(price, precision)
    price = newPrice
    print("priiiiiiiiiiiiiiiiice   ",price)
    return price


def save_to_excel(data_frame, out_path = 'C;/TEST.xlsx', sheet_name='Sheet 1'):
    # crea o sobreescriu un excel amb la informació del dataframe data_frame
    # out_path nome del fitxer (amb path: C:/xxx/yyy.xlsx)
    # sheeet_name nom del sheet
    try:
        writer = pd.ExcelWriter(out_path, engine='xlsxwriter')
        data_frame.to_excel(writer, sheet_name)
        writer.save()
    except IOError as e:
        error_handling(e,'I/O Error: ')
        raise
    except Exception as e:
        error_handling(e)
        raise


def error_handling (e, initial_text='Exception'):
    #mostra un pop-up d'error
    root = tk.Tk()
    root.withdraw()
    text=initial_text + 'Error Number({0}): {1}'.format(e.errno, e.strerror)
    msgbox.showerror("Error", text)


def portfolio_to_dict(ib_):
    try:
        pfl = ib_.portfolio()
        # dictionary de lists que contindrà les dades que volem recuperar de l'objecte Contract per cada PortfolioItem del Portfolio
        d_contr = {'secType': [], 'conId': [], 'symbol': [], 'exchange': [], 'primaryExchange': [], 'currency': [], 'localSymbol': []}
        # OrderedDict de lists que contindrà les dades que volem recuperar de la namedtupla PortfolioItem (excepte els detalls del Contract) per cada PortfolioItem del Portfolio del Portfolio
        d_pfl = {'position': [], 'marketPrice': [], 'marketValue': [],'averageCost': [], 'unrealizedPNL': [], 'realizedPNL': [], 'account': []}
        # recorrem tots els PortfoioItema Portfolio
        for i in range(len(pfl)):
            ib_.qualifyContracts(pfl[i].contract)
            for k in d_contr.keys():
                # afegim els valors (cada value de (key,value) és una llista) de cada atribut que recuperem de l'objecte Contract d'aquest PortfolioItem.
                d_contr[k].append(getattr(pfl[i].contract, k))
            for k in d_pfl.keys():
                # afegim els valors (cada value de (key,value) és una llista) de cada valor que m'interessa de Portfolio Item ( a part dels detalls del contracte, recuperats abans)
                d_pfl[k].append(getattr(pfl[i], k))
        # posem tota la informació al dictionary pfl_values
        d_pfl.update(d_contr)
        # ordenem i retornem un OrderedDict
        my_order=['conId', 'symbol', 'localSymbol', 'currency', 'secType', 'position', 'averageCost', 'marketPrice', 'marketValue', 'unrealizedPNL', 'realizedPNL']
        od_pfl = OrderedDict((k, d_pfl[k]) for k in my_order)
        return od_pfl
    except Exception as e:
        msg='Exception in function portfolio_to_dict \n'
        error_handling(e, msg)
        raise


def open_workbook(filepath, sheetname):
    wb = oppy.load_workbook(filepath, read_only=False, keep_vba=True)  # to open the excel sheet and if it has macros
    sheet = wb.get_sheet_by_name(sheetname)
    print(sheet['D2'].value)
    sheet.cell(row=2, column=4).value = 'sample'
    print(sheet['D2'].value)
    wb.save(filepath)


def makeform(root, fields):
    entries = []
    print('fields1: 1: ', fields)
    for field in fields:
        row = Frame(root)
        lab = Label(row, width=15, text=field[0], anchor='w')
        v = StringVar(root, value=field[1])
        ent = Entry(row,textvariable=v)
        row.pack(side=TOP, fill=X, padx=5, pady=5)
        lab.pack(side=LEFT)
        ent.pack(side=RIGHT, expand=YES, fill=X)
        entries.append((field, ent.get()))
        print ('field: ', field)
        field[1] = ent.get()
    print('fields: ', fields)
    return fields


if __name__ == '__main__':

    # determinem paràmetres gestió portfolio
    pctProfitTarget = 80

    # creem instància de IB()
    ib = IB()

    # connectem a IBAPI
    host = '127.0.0.1'
    port = 7497
    clientid = 3
    ibConnect(host, port, clientid)

    # posem les dades que m'interessa recuperar en un OrderedDict de Lists
    odict=OrderedDict(portfolio_to_dict(ib))

    #for id in odict['conId']:
    #    c=Contract(conId=id)
    #    ib.qualifyContracts(c)
    #    ib.reqMktData(c)
    #    print(c)
        #t = ib.reqTickers(c)
       #print (t)

    # convertim el diccionari en un dataframe
    df = pd.DataFrame.from_dict(odict)
    print(list(df.columns))
    # el guardem en Excel
    #save_to_excel(df, 'C:/TWS API/Besuga/BesugaPortfolio.xlsx', 'Besuga')

    dfs = pd.read_excel('C:/TWS API/Besuga/BesugaPortfolio.xlsx', sheet_name=0)
    dfs2=dfs.set_index('Index', drop = 'False')
    print(dfs2)
    pfl2=ib.portfolio()
    for index, row in dfs2.iterrows():
        contr = Contract(conId = row['conId'])
        ib.reqMktData(contr, '', False, False)
        ib.qualifyContracts(contr)
        print(contr)
        ticker = ib.ticker(contr)
        print (ticker)



    # analitzem Accoount
    #accountAnalysis()

    # analitzem posicions obertes
    #portfolioAnalysis()

    # desconnectem de IBAPI
    ibDisconnect()