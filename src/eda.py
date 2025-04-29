
import polars as pl
import pandas as pd
import os
from matplotlib.backends.backend_pdf import PdfPages
import matplotlib.pyplot as plt
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
#Datos para la construccion del EDA
TOTAL_SALES = "TOTAL VENTAS"
SALES_MEAN = "PROMEDIO"
SALES_MEDIAN = "MEDIANA"
SALES_STDDEV = "DESVIACION ESTANDAR"
SALES_MIN = "VENTA MINIMA"
SALES_MAX = "VENTA MAXIMA"


def cleanDateColumn(df: pl.DataFrame, dateColumnName: str) -> pl.DataFrame:
    """
    Converts the specified column in a DataFrame from string format to a proper Datetime type.

    :param df: A pl.DataFrame containing the date column
    :param dateColumnName: Name of the column to convert
    :return: A pl.DataFrame with the specified column converted to Datetime type
     
    Example usage:
    >>> dfCleaned = cleanDateColumn(dfSalesMerged, DATE)
    >>> print(dfCleaned)
    """
    if not isinstance(df, pl.DataFrame):
        raise TypeError("The 'df' parameter must be a pl.DataFrame.")

    if dateColumnName not in df.columns:
        raise ValueError(f"The column '{dateColumnName}' does not exist in the DataFrame.")

    return df.with_columns([
        pl.col(dateColumnName)
        .str.strptime(pl.Datetime, format="%b %e %Y %I:%M%p")
        .alias(dateColumnName)
    ])


def saveFigureToPdf(fig, pdf: PdfPages) -> None:
    """
    Saves a matplotlib figure into a PDF file with a white background.

    :param fig: A matplotlib figure object
    :param pdf: A PdfPages object to save the figure
    """
    pdf.savefig(fig, facecolor='white', edgecolor='white')
    plt.close(fig)

def prepareSummaryStatistics(df: pl.DataFrame) -> pd.DataFrame:
    """
    Prepares summary statistics for the sales prices grouped by warehouse and ticket (Warehouse + NoMov):
    - Mean
    - Median
    - Standard Deviation
    - Minimum
    - Maximum
    All values are rounded to 2 decimal places.

    :param df: A Polars DataFrame
    :return: A Pandas DataFrame with the summary statistics
    """
    # 1. Aggregate total sale per warehouse and ticket (Warehouse + NoMov)
    ticketSales = df.group_by([WAREHOUSE, NO_MOV]).agg([
        pl.col(SALE_PRICE).sum().alias(TOTAL_SALES)
    ])

    # 2. Calculate summary statistics over the ticket totals
    summary = ticketSales.select([
        pl.col(TOTAL_SALES).mean().alias(SALES_MEAN),
        pl.col(TOTAL_SALES).median().alias(SALES_MEDIAN),
        pl.col(TOTAL_SALES).std().alias(SALES_STDDEV),
        pl.col(TOTAL_SALES).min().alias(SALES_MIN),
        pl.col(TOTAL_SALES).max().alias(SALES_MAX)
    ])
    
    # 3. Convert manually to Pandas DataFrame
    summaryDf = pd.DataFrame(summary.to_dict(as_series=False))
    
    # 4. Round all numeric values to 2 decimal places
    summaryDf = summaryDf.round(2)
    
    return summaryDf

def prepareTopSellers(
    df: pl.DataFrame,
    limit: int = 5,
    ascending: bool = False,
    days: int = 30
) -> pd.DataFrame:
    """
    Prepares the top or bottom sellers by total sales within the last 'days' from the latest recorded date.

    :param df: A Polars DataFrame containing sales data
    :param limit: Number of sellers to return (default 5)
    :param ascending: Sort in ascending order for bottom sellers, or descending for top sellers (default False)
    :param days: Number of days to look back from the latest date to filter sales data (default 30)
    :return: A Pandas DataFrame with the top or bottom sellers
    """
    # 1. Filter the DataFrame to keep only the last 'days' from the maximum date in the dataset
    df = plu.filterLastNDaysFromMaxDate(df, days=days, columnDate=DATE)

    # 2. Group by seller and calculate total sales
    topSellers = df.group_by(SELLER).agg([
        pl.col(SALE_PRICE).sum().alias(TOTAL_SALES)
    ]).sort(TOTAL_SALES, descending=not ascending)

    # 3. Take only the top or bottom 'limit' sellers
    topSellers = topSellers.head(limit)

    # 4. Manual conversion to Pandas DataFrame
    return pd.DataFrame(topSellers.to_dict(as_series=False))

def prepareSalesByDay(df: pl.DataFrame, days: int = 30) -> pd.DataFrame:
    """
    Aggregates total sales per day, limited to the most recent N days.

    :param df: A Polars DataFrame containing sales data
    :param days: Number of most recent days to include (default is 30)
    :return: A Pandas DataFrame with total sales aggregated by day
    """
    # 1. Filter the DataFrame to include only the last 'days' from the most recent date
    df = plu.filterLastNDaysFromMaxDate(df, days=days, columnDate=DATE)

    # 2. Group sales by date and calculate total sales per day
    salesByDay = df.group_by(pl.col(DATE).dt.date()).agg([
        pl.col(SALE_PRICE).sum().alias(TOTAL_SALES)
    ]).sort(DATE)

    # 3. Convert manually to Pandas DataFrame
    return pd.DataFrame(salesByDay.to_dict(as_series=False))

def prepareSalesDistributions(df: pl.DataFrame, days: int = 30) -> None:
    """
    Genera histogramas y diagramas de caja (boxplots) para los totales de ventas por ticket (NO_MOV) y almacén.
    Utiliza escala original y logarítmica, y muestra etiquetas en valores reales incluso en escala log.
    Filtra solo los últimos N días desde la fecha más reciente.

    :param df: Un DataFrame de Polars
    :param days: Número de días recientes a considerar (por defecto 30)
    :yield: Figuras de Matplotlib listas para guardar en PDF
    """
    import numpy as np

    # 1. Filtrar últimos N días
    df = plu.filterLastNDaysFromMaxDate(df, days=days, columnDate=DATE)

    # 2. Agrupar por NO_MOV y WAREHOUSE, sumar SALE_PRICE
    grouped = df.group_by([NO_MOV, WAREHOUSE]).agg([
        pl.col(SALE_PRICE).sum().alias(TOTAL_SALES)
    ])

    # 3. Convertir a pandas
    salesData = pd.DataFrame(grouped.select(TOTAL_SALES).to_dict(as_series=False))

    # 4. Histograma - Escala logarítmica con etiquetas reales
    salesDataLog = salesData.copy()
    salesDataLog[TOTAL_SALES] = salesDataLog[TOTAL_SALES].apply(lambda x: np.log1p(x))

    fig2, ax2 = plt.subplots(facecolor="white")
    salesDataLog.hist(bins=50, ax=ax2, color="salmon", edgecolor="black")
    ax2.set_title(f"Histograma (últimos {days} días)", fontsize=16)
    ax2.set_xlabel("Precio de Venta")
    ax2.set_ylabel("Frecuencia")

    # Etiquetas del eje X transformadas
    ticks = [0, 2, 4, 6, 8, 10]
    labels = [f"${int(np.expm1(t)):,}" for t in ticks]
    ax2.set_xticks(ticks)
    ax2.set_xticklabels(labels)

    plt.grid(True, linestyle="--", linewidth=0.5)
    plt.tight_layout()
    yield fig2

    # 5. Boxplot - Escala logarítmica con etiquetas reales
    fig4, ax4 = plt.subplots(facecolor="white")
    salesDataLog.boxplot(ax=ax4)
    ax4.set_title(f"Boxplot (últimos {days} días)", fontsize=16)
    ax4.set_ylabel("Precio de Venta")

    y_ticks = [0, 2, 4, 6, 8, 10]
    y_labels = [f"${int(np.expm1(y)):,}" for y in y_ticks]
    ax4.set_yticks(y_ticks)
    ax4.set_yticklabels(y_labels)

    plt.grid(True, linestyle="--", linewidth=0.5)
    plt.tight_layout()
    yield fig4


def generateSalesEDA(df: pl.DataFrame, outputPdfPath: str, days: int = 30) -> None:
    """
    Generates the Exploratory Data Analysis (EDA) for sales data and saves it to a PDF.

    :param df: A Polars DataFrame
    :param outputPdfPath: Path where the PDF will be saved
    """
    os.makedirs(os.path.dirname(outputPdfPath), exist_ok=True)
    pdf = PdfPages(outputPdfPath)
    
    # 1. Estadísticas Resumen
    summaryStats = prepareSummaryStatistics(df)
    summaryDict = summaryStats.to_dict(orient="records")[0]
    fig, ax = plt.subplots(figsize=(8.5, 11), facecolor="white")
    # Datos
    labels = list(summaryDict.keys())
    values = list(summaryDict.values())
    # Barras horizontales
    bars = ax.barh(labels, values, color="skyblue", edgecolor="black")
    # Escala log en X
    ax.set_xscale("log")
    # Etiquetas dentro de cada barra (valores reales con formato bonito)
    for bar, value in zip(bars, values):
        ax.text(value, bar.get_y() + bar.get_height() / 2,
                f"${int(value):,}", va="center", ha="left", fontsize=9, color="black")
    # Personalización
    plt.title("Estadísticas Resumen de Precios de Venta", fontsize=20)
    plt.xlabel("Valor", fontsize=14)
    # Etiquetas del eje X en formato real (aunque esté log)
    ticks = [1, 10, 100, 1_000, 10_000, 100_000]
    labels = [f"${tick:,}" for tick in ticks]
    ax.set_xticks(ticks)
    ax.set_xticklabels(labels)

    plt.grid(True, axis='x', linestyle="--", linewidth=0.5)
    plt.yticks(rotation=45, fontsize=6, ha="right", rotation_mode="anchor")
    plt.tight_layout()

    # Guardar al PDF
    saveFigureToPdf(fig, pdf)

    # 2. Ventas por día
    salesByDay = prepareSalesByDay(df, days=days)
    fig, ax = plt.subplots(facecolor="white")
    salesByDay.plot(x=DATE, y=TOTAL_SALES, kind="line", ax=ax)
    plt.title(f"Ventas Totales por Día (últimos {days} días)", fontsize=16)
    plt.xlabel("Fecha", fontsize=10)
    plt.ylabel("Ventas Totales", fontsize=12)
    plt.xticks(rotation=45)
    plt.grid()
    plt.tight_layout()
    saveFigureToPdf(fig, pdf)

    # 3. Mejores vendedores
    topBestSellers = prepareTopSellers(df, limit=20, ascending=False, days=days)
    fig, ax = plt.subplots(figsize=(8.5, 11), facecolor="white")
    ax.set_facecolor("white")
    topBestSellers.plot(x=SELLER, y=TOTAL_SALES, kind="bar", ax=ax, color="skyblue", edgecolor="black")
    plt.title(f"Top 20 Mejores Vendedores (últimos {days} días)", fontsize=20)
    plt.xlabel("Vendedor", fontsize=8)
    plt.ylabel("Ventas Totales", fontsize=14)
    plt.xticks(rotation=45, fontsize=6, ha="right", rotation_mode="anchor")
    plt.yticks(fontsize=12)
    plt.grid(True, color="grey", linestyle="--", linewidth=0.5)
    plt.tight_layout()
    saveFigureToPdf(fig, pdf)

    # 4. Peores vendedores
    topWorstSellers = prepareTopSellers(df, limit=20, ascending=True, days=days)
    fig, ax = plt.subplots(figsize=(8.5, 11), facecolor="white")
    ax.set_facecolor("white")
    topWorstSellers.plot(x=SELLER, y=TOTAL_SALES, kind="bar", ax=ax, color="skyblue", edgecolor="black")
    plt.title(f"Top 20 Peores Vendedores (últimos {days} días)", fontsize=20)
    plt.xlabel("Vendedor", fontsize=8)
    plt.ylabel("Ventas Totales", fontsize=14)
    plt.xticks(rotation=45, fontsize=6, ha="right", rotation_mode="anchor")
    plt.yticks(fontsize=12)
    plt.grid(True, color="grey", linestyle="--", linewidth=0.5)
    plt.tight_layout()
    saveFigureToPdf(fig, pdf)

    # 4. Sales distributions
    for fig in prepareSalesDistributions(df, days=days):
        saveFigureToPdf(fig, pdf)

    # Close the PDF
    pdf.close()






