#General Parameters
myprefexchange = "SMART"
myclientid = 88

#Capital-related
mymaxposition = 10000       # per calcular el nombre de posicions a obrir: (mymaxposition/(100*preu acció*Delta)

# DB params
dbhost = 'localhost'
dbname = 'besuga'
dbuser = 'xarnaus'
dbpassword = 'Besuga8888'

# Scan params
#myscancodelist = ['HIGH_VS_52W_HL', 'LOW_VS_52W_HL']
#myscancodelist = ['LOW_VS_52W_HL']
myscanmaxstocks = 5          # limits the number of stocks returned by the scan
myscaninstrument = 'STK'
myscanlocation = 'STK.US.MAJOR'
myscanvolabove= 200000
myscanmktcapabove = 10000000000
myscanavgvoloptabove = 10000

# open/close parameters
mydaystoearnings = 1        # Closse all positions at most mydaystoearnings to the Earnings Date

# Options
myoptdaystoexp = 45         # desired option days to expiration
myoptprofit = 80            # tanquem posicions al mypctprofit% de guany
myoptprofit10 = 30          # tanquem posicions al mypctprofit10% de guany si el temps transcorregut <= 10%
myoptprofit20 = 40          # tanquem posicions al mypctprofit40% de guany si el temps transcorregut <= 40%
myoptprofit50 = 65          # tanquem posicions al mypctprofit65% de guany si el temps transcorregut <= 65%
myoptprofit75 = 75          # tanquem posicions al mypctprofit75% de guany si el temps transcorregut <= 75%
myoptlossdef = -75          # per opcions vengudes: obrim una defensiva al myoptlossdef de pèrdues
myoptloss = -65             # tanquem posicions al myoptloss% de pèrdues

# Stock
mystkprofit = 20            # tanquem posicions al mystkprofit% de guany
mystkloss = -20             # tanquem posicions al mystkloss% de pèrdua

# High/Low parameters
my52whighfrac = 0.99        # distància del preu actual al 52w high (0.99 vold dir que està a un 1% per sota)
my52wlowfrac = 0.01         # distància del preu actual al 52w low (0.01 vold dir que està a un 1% per sobre)

