
import eda as eda
import polarsUtils as plu

#Columnas para trabajar
WAREHOUSE = "Almacen"
DATE = "Fecha"
CUSTOMER = "Cliente"
SELLER = "Vendedor"
PRODUCT_CONCAT = "ProdConcat"
QUANTITY = "Cantidad"
SALE_PRICE = "PrecioVenta"
PAYMENT_METHODS = "Metodos De Pago"
NO_MOV = "NoMov"

#Datos para la construccion del DataFrame
DIRECTORY = "./data/sales"
KEYWORDS = "Ventas por Tickets"
SALES_COLUMNS =  [
    WAREHOUSE,
    NO_MOV,
    DATE,
    CUSTOMER,
    SELLER,
    PRODUCT_CONCAT,
    QUANTITY,
    SALE_PRICE,
    PAYMENT_METHODS
]
OUTPUT_PATH = "./output"
OUTPUT_FILE_NAME = "salesReportMerged"


dfSalesMerged = plu.createMergedDataFrameFromExcelMatch(directory= DIRECTORY, keyword= KEYWORDS, columns= SALES_COLUMNS, saveToExcel= False, outputPath= OUTPUT_PATH, outputFileName= OUTPUT_FILE_NAME)

dfSalesMerged = eda.cleanDateColumn(dfSalesMerged, DATE)

eda.generateSalesEDA(dfSalesMerged, "./output/bi/salesEDAReport.pdf", days=90)

#plu.saveDataFrameToExcel(dfSalesMerged, OUTPUT_PATH, OUTPUT_FILE_NAME)
