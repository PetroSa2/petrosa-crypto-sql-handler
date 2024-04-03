import mysql.connector
import datetime
import time


cnx = mysql.connector.connect(user='xxx', password='xxx',
                              host='xxx',
                              database='xxx')

candle = {}
candle['datetime'] = datetime.datetime.now()
candle['ticker'] = "BTCTEST"
candle['open'] = 12345
candle['high'] = 54321
candle['low'] = 11111
candle['close'] = 222222
candle['qty'] = float(123487.23423)
candle['vol'] = float(1234.321)
candle['close_time'] = datetime.datetime.now()
candle['closed_candle'] = True
candle['origin'] = "fromtest"
candle['timestamp'] = int(time.time() * 1000)
candle['insert_time'] = datetime.datetime.utcnow()

mycursor = cnx.cursor()

sql = "INSERT INTO `generic` (`datetime`, `ticker`, `open`, `high`, `low`, `close`, `qty`, `vol`, `close_time`, `closed_candle`, `origin`, `timestamp`, `insert_time`) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
val = ("John", "Highway 21")
mycursor.execute(sql, list(candle.values()))

cnx.commit()
