class sp500(object):

    def __init__(self, source='sp500_standard'):
        self.source = source
        self.method_list = {'finsymbol': self.finsymbol, 'sp500_standard': self.sp500_standard}
        self.method = self.method_list[self.source]

    @staticmethod
    def finsymbol():
        import finsymbols
        stock_list = []
        stock_info = finsymbols.get_sp500_symbols()
        for i in range(len(stock_info)):
            stock_list.append(stock_info[i]['symbol'])
        return stock_list

    @staticmethod
    def sp500_standard():
        import urllib
        stock_list = []
        source = "http://www.spindices.com/documents/additional-material/sp-500-eps-est.xlsx?force_download=true"
        xlspath = '/tmp/sp500.xls'
        urllib.urlretrieve(source, xlspath)
        import xlrd
        workbook = xlrd.open_workbook(xlspath)
        # print workbook.sheet_names()
        worksheet = workbook.sheet_by_name('ISSUES')
        num_rows = worksheet.nrows - 1
        curr_row = -1
        for curr_row in range(6, num_rows,1):
            curr_row += 1
            row = worksheet.row(curr_row)
            curr_cell = 0
            # Cell Types: 0=Empty, 1=Text, 2=Number, 3=Date, 4=Boolean, 5=Error, 6=Blank
            cell_type = worksheet.cell_type(curr_row, curr_cell)
            cell_value = worksheet.cell_value(curr_row, curr_cell)
            stock_list.append(cell_value)
        return stock_list