








class Table():
    tableData = []
    height = 0
    width = 0
    headerHeight = 0
    headerWidth = 0
    
    def __init__(self, height, width, headerHeight, headerWidth):
        self.height = height
        self.width = width
        self.headerHeight = headerHeight
        self.headerWidth = headerWidth


    def setTableData(self, tableData):
        self.tableData = tableData


    def setCellData(self, row, column, cellData):
        self.tableData[row][column] = cellData