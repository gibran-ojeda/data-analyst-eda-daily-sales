import polars as pl
import pandas as pd
import os
from datetime import datetime
from typing import List, Optional
import systemUtils as sysUtils

def readExcelToDataFrame(path: str, sheetName: Optional[str] = None) -> pl.DataFrame:
    """
    Reads an Excel (.xlsx) file and returns it as a Polars DataFrame.

    :param path: Full path to the Excel file
    :param sheetName: Name of the sheet to read (default is the first sheet)
    :return: A pl.DataFrame object

    Example usage:
    >>> df = readExcelToDataFrame("./data/salesReport.xlsx")
    >>> print(df)

    >>> dfSheet = readExcelToDataFrame("./data/salesReport.xlsx", sheetName="JanuarySales")
    >>> print(dfSheet)
    """
    if not path.endswith('.xlsx'):
        raise ValueError("The file must have a .xlsx extension.")
    
    if not os.path.exists(path):
        raise FileNotFoundError(f"The file '{path}' does not exist.")
    
    try:
        # Try using Polars native read_excel
        dfPolars = pl.read_excel(path, sheet_name=sheetName)
    except (AttributeError, TypeError):
        # Fallback if Polars version does not support read_excel or sheet_name argument
        dfPandas = pd.read_excel(path, sheet_name=sheetName)
        dfPolars = pl.from_pandas(dfPandas)

    return dfPolars

def unionDataFrames(dfList: List[pl.DataFrame]) -> pl.DataFrame:
    """
    Merges a list of Polars DataFrames into a single DataFrame.

    :param dfList: List of pl.DataFrame objects
    :return: A single pl.DataFrame resulting from concatenating all the DataFrames

    Example usage:
    >>> df1 = pl.DataFrame({"a": [1, 2], "b": [3, 4]})
    >>> df2 = pl.DataFrame({"a": [5, 6], "b": [7, 8]})
    >>> result = unionDataFrames([df1, df2])
    >>> print(result)
    """
    if not dfList:
        raise ValueError("The DataFrame list is empty.")
    
    if not all(isinstance(df, pl.DataFrame) for df in dfList):
        raise TypeError("All elements in the list must be of type pl.DataFrame.")
    
    return pl.concat(dfList)

def saveDataFrameToExcel(df: pl.DataFrame, path: str, fileName: str) -> None:
    """
    Converts a Polars DataFrame to Excel and saves it to the specified path.

    :param df: A pl.DataFrame object
    :param path: Directory path where the Excel file will be saved
    :param fileName: Output filename without extension

    Example usage:
    >>> df = pl.DataFrame({"a": [1, 2], "b": [3, 4]})
    >>> saveDataFrameToExcel(df, "./outputs", "myReport")
    """
    if not isinstance(df, pl.DataFrame):
        raise TypeError("The 'df' parameter must be a pl.DataFrame.")

    if not fileName.endswith('.xlsx'):
        fileName += '.xlsx'

    os.makedirs(path, exist_ok=True)

    # Step 1: Try converting normally
    try:
        dfPandas = df.to_pandas()
    except Exception as e:
        print(f"Warning: Direct to_pandas() failed. Trying manual conversion. Error: {e}")
        # Step 2: Manual fallback: recreate a Pandas DataFrame manually
        dfPandas = pd.DataFrame({col: df[col].to_list() for col in df.columns})

    # Step 3: Save as Excel
    outputPath = os.path.join(path, fileName)
    dfPandas.to_excel(outputPath, index=False)

def selectColumns(df: pl.DataFrame, columnsToKeep: List[str]) -> pl.DataFrame:
    """
    Selects specific columns from a Polars DataFrame and returns a new DataFrame.

    :param df: A pl.DataFrame object
    :param columnsToKeep: List of column names to keep
    :return: A pl.DataFrame with only the specified columns

    Example usage:
    >>> df = pl.DataFrame({
    ...     "name": ["Alice", "Bob", "Charlie"],
    ...     "age": [25, 30, 35],
    ...     "city": ["NYC", "LA", "Chicago"]
    ... })
    >>> selectedDf = selectColumns(df, ["name", "city"])
    >>> print(selectedDf)
    """
    if not isinstance(df, pl.DataFrame):
        raise TypeError("The 'df' parameter must be a pl.DataFrame.")
    
    if not all(isinstance(col, str) for col in columnsToKeep):
        raise TypeError("All elements in 'columnsToKeep' must be strings.")
    
    missingColumns = [col for col in columnsToKeep if col not in df.columns]
    if missingColumns:
        raise ValueError(f"The following columns are not present in the DataFrame: {missingColumns}")
    
    return df.select(columnsToKeep)

def replaceNaNWithZero(df: pl.DataFrame) -> pl.DataFrame:
    """
    Replaces NaN or null values with 0 in all numeric columns of a Polars DataFrame.

    :param df: A pl.DataFrame object to process
    :return: A new pl.DataFrame with NaN/null values replaced by 0 in numeric columns only

    Example usage:
    >>> df = pl.DataFrame({
    ...     "sales": [100, None, 300],
    ...     "profit": [50, 75, None],
    ...     "city": ["NYC", "LA", "Chicago"]
    ... })
    >>> updatedDf = replaceNaNWithZero(df)
    >>> print(updatedDf)
    """
    if not isinstance(df, pl.DataFrame):
        raise TypeError("The 'df' parameter must be a pl.DataFrame.")

    numericColumns = [col for col, dtype in zip(df.columns, df.dtypes) if dtype in (pl.Int8, pl.Int16, pl.Int32, pl.Int64, pl.UInt8, pl.UInt16, pl.UInt32, pl.UInt64, pl.Float32, pl.Float64)]
    
    df = df.with_columns(
        [pl.col(col).fill_null(0).alias(col) for col in numericColumns]
    )
    
    return df

def filterLastNDaysFromMaxDate(df: pl.DataFrame, days: int, columnDate: str = "Fecha") -> pl.DataFrame:
    """
    Filters the DataFrame to keep only the last N days from the maximum registered date in the DATE column.

    :param df: A Polars DataFrame
    :param days: Number of days to go back from the latest date
    :return: A filtered Polars DataFrame
    """
    from datetime import timedelta

    if not isinstance(df, pl.DataFrame):
        raise TypeError("The 'df' parameter must be a Polars DataFrame.")

    if columnDate not in df.columns:
        raise ValueError(f"The column '{columnDate}' does not exist in the DataFrame.")

    # 1. Find the maximum date in the dataset
    maxDate = df.select(pl.col(columnDate).max()).item()

    if not isinstance(maxDate, datetime):
        raise ValueError(f"The maximum value in column '{columnDate}' is not a datetime.")

    # 2. Calculate cutoff date
    cutoffDate = maxDate - timedelta(days=days)

    # 3. Filter the DataFrame
    filteredDf = df.filter(pl.col(columnDate) >= cutoffDate)

    return filteredDf


def createMergedDataFrameFromExcelMatch(
    directory: str = "./",
    keyword: str = "",
    columns: Optional[List[str]] = None,
    sheetName: Optional[str] = None,
    saveToExcel: bool = False,
    outputPath: Optional[str] = None,
    outputFileName: str = "mergedReport"
) -> pl.DataFrame:
    """
    Searches for Excel files in a directory that match a keyword,
    reads them into DataFrames (optionally filtering columns and selecting a specific sheet),
    merges them into a single DataFrame, and optionally saves it as an Excel file.

    :param directory: Directory path to search for Excel files (default "./")
    :param keyword: Keyword to match in filenames
    :param columns: List of columns to select from each file (optional)
    :param sheetName: Name of the sheet to read (optional)
    :param saveToExcel: Whether to save the merged DataFrame to an Excel file (default False)
    :param outputPath: Directory where the Excel file will be saved (required if saveToExcel=True)
    :param outputFileName: Name of the output Excel file without extension (default "mergedReport")
    :return: A single merged pl.DataFrame from all successfully read files
    :raises FileNotFoundError: If no matching files are found
    :raises ValueError: If no DataFrames could be read or merged
     
    Example usage:
    >>> df = createMergedDataFrameFromExcelMatch("./data", "sales", ["id", "amount"], "January", True, "./outputs", "salesJanuary")
    >>> print(df)
    """
    # 1. Find matching Excel files
    matchingFiles = sysUtils.findExcelFilesByMatch(directory, keyword, extension="xlsx")
    
    if not matchingFiles:
        raise FileNotFoundError(f"No Excel files matching '{keyword}' were found in '{directory}'.")

    # 2. Read all files into DataFrames
    dataFrames = []
    for fileName in matchingFiles:
        fullPath = os.path.join(directory, fileName)
        try:
            df = readExcelToDataFrame(fullPath, sheetName=sheetName)
            if columns:
                df = selectColumns(df, columns)
            dataFrames.append(df)
        except Exception as e:
            print(f"Warning: Could not process file '{fileName}': {e}")

    # 3. Validate at least one DataFrame was read
    if not dataFrames:
        raise ValueError("No valid DataFrames could be created from the matching files.")

    # 4. Merge all DataFrames
    mergedDataFrame = unionDataFrames(dataFrames)

    # 5. Save to Excel if requested
    if saveToExcel:
        if outputPath is None:
            raise ValueError("outputPath must be provided when saveToExcel=True.")
        saveDataFrameToExcel(mergedDataFrame, outputPath, outputFileName)

    return mergedDataFrame