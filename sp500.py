import finsymbols

class sp500(object):

    def __init__(self):
        pass

    @staticmethod
    def symbol_list():
        stock_list=[]
        stock_info = finsymbols.get_sp500_symbols()
        for i in range(len(stock_info)):
            stock_list.append(stock_info[i]['symbol'])
        return stock_list