"""Evaulate historical volatility and implied volatility"""
import numpy
import scipy

_TRADING_DAYS = 252

def historical_volatility(stock_price, lookback_num=30, time_unit=1, date=None):
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
