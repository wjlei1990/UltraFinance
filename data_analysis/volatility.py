"""Evaulate historical volatility and implied volatility"""
import numpy
import scipy
import sys
sys.path.append('../')
from data_service.StockData import StockClient 

_TRADING_DAYS = 252

def get_hist_vol(ticket, lookback_num=30, limit=_TRADING_DAYS):
    """
    Get historical volatitive for a specific `ticket` for the last `limit`
    number of days use the default data source.
    
    INPUTS:
    ticket: string, specify stock ticket, for example, 'AAPL'
    lookback_num: number of days to evaulate historical vol
    limit: an integer, how many days to evaulate for.
           default is one year (252 days)
           if limit is 0 or negative, use the whole data avialable

    OUTPUTS:
    vol: numpy array, historical volatility
    t:   list, date corresponds to vol
    adj_close: adjusted close price for the same period
    """
    fromsql   = StockClient('apc524','apc524','stockprice',host='junyic.net')
    stockdata = fromsql.read_full_stock_record([ticket])
    stockdata = stockdata[ticket]
    if stockdata is None:
        raise ValueError('Ticket does not exist')
   
    adj_close = stockdata['Adj Close']
    t         = stockdata['Date']
    if limit > 0:
        try:
            adj_close = adj_close[-(limit+lookback_num-1):]
            t         = t[-(limit+lookback_num-1):]
        except IndexError:
            raise IndexError('data too short to calculate volatility: \
                              try smaller lookback_num')

    vol, t = cal_historical_volatility(adj_close, lookback_num, 1,t)
    return vol, list(t), adj_close


def cal_historical_volatility(stock_price, lookback_num=30, time_unit=1, date=None):
    """
    Evaulate annulized historical volatility given a list of stock price, 
    which is the standard deviation of the instrument's yearly logarithmic 
    returns. 

    INPUTS:
    stock_price: a list of stock price. Default sampling frequency is daily
    lookback_num: number of sample used to look back
    time_unit: default is 1, meaning daily sample. Set to 5 if sample is weekly
               price
    date: date for the corresonding stock_price samples

    OUTPUTS:
    sigma: a list of volatility. Size if len(stock_price) - lookback_num+1
    t: optional, dates corresponds to volatility

    EXCEPTIONS:
    throws ValueError if inputs parameters are wrong
    """
    if (len(stock_price) < lookback_num):
        raise ValueError('look back period cannot be larger than stock_price length')
    if (lookback_num < 3):
        raise ValueError('look back period cannot be smaller than 3')

    stock_price = numpy.array(stock_price)
    volatility  = numpy.zeros(len(stock_price)-lookback_num+1)
    # daily log return
    log_return  = scipy.log(stock_price[1:]/stock_price[0:-1])/time_unit 
     
    for i in xrange(0, len(stock_price)-lookback_num+1):
        ref_log_return = log_return[i:i+lookback_num-1]
        volatility[i] = ref_log_return.std()*scipy.sqrt(_TRADING_DAYS)
        
    if date is None:
        return volatility
    else:
        return volatility, date[lookback_num-1:]
