#General Parameters
myprefexchange = "SMART"
mycurrency = "USD"
myclientid = 88

#Capital-related
mymaxposition = 10000       # per calcular el nombre de posicions a obrir: (mymaxposition/(100*preu acció*Delta)

# DB params
dbhost = 'localhost'
dbname = 'besuga'
dbuser = 'xarnaus'
dbpassword = 'Besuga8888'

# Scan params
myscanmaxstocks = 50          # limits the number of stocks returned by the scan
myscaninstrument = 'STK'
myscanlocation = 'STK.US.MAJOR'
myscanvolabove= 200000
myscanmktcapabove = 10000000000
myscanavgvoloptabove = 10000
# Wikipedia params ('http://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
myurl = 'http://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
myprdnum =  5               # prdnum = número de periodes (1,2,10,35, etc)
myprdscale = "Y"            # prdscale = unitat en la que es mesuren els periodes:(S(segons),D (dia), W(semana), M(mes), Y(any)
mybarsixe = "1 week"        # barsize: duració temporal de cada "bar": "1 secs", "30 mins", "1 hour", "1 day", "1 week", "1 month"

# open/close parameters
myaction = 'SELL'               # Determina si comprem (BUY), venem (SELL) o una de cada (BOTH)
mydaystoearnings = 1            # Close all positions at most mydaystoearnings to the Earnings Date
mydefaultearndate = 20880808    # Earning Date per defecte (quan no n'entrem cap)

# Options
myoptbuyitm = 0.1           # Distància de l'strike (percentual entre -1 i 1, 0 = strike, positius =ITM) per accions comprades
myoptsellitm = 0            # Distància de l'strike (percentual entre -1 i 1, 0 = strike, positius =ITM) per accions  venudes
myoptselldte = 45           # desired option days to expiration when selling options
myoptbuydte = 180           # desired option days to expiration when buying options
myoptprofit = 80            # tanquem posicions al mypctprofit% de guany
myoptprofit10 = 30          # tanquem posicions al mypctprofit10% de guany si el temps transcorregut <= 10%
myoptprofit20 = 40          # tanquem posicions al mypctprofit40% de guany si el temps transcorregut <= 20%
myoptprofit50 = 65          # tanquem posicions al mypctprofit65% de guany si el temps transcorregut <= 50%
myoptprofit75 = 75          # tanquem posicions al mypctprofit75% de guany si el temps transcorregut <= 75%
myoptlossdef = -75          # per opcions vengudes: obrim una defensiva al myoptlossdef de pèrdues
myoptloss = -50            # tanquem posicions al myoptloss% de pèrdues

# Stock
mystkprofit = 20            # tanquem posicions al mystkprofit% de guany
mystkloss = -20             # tanquem posicions al mystkloss% de pèrdua

# High/Low parameters
my52whighfrac = 0.99        # distància del preu actual al 52w high (0.99 vold dir que està a un 1% per sota)
my52wlowfrac = 0.01         # distància del preu actual al 52w low (0.01 vold dir que està a un 1% per sobre)

