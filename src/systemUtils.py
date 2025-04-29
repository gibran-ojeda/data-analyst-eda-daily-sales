import shutil
import os
import fnmatch
from datetime import datetime
from typing import List, Optional

def findExcelFilesByMatch(directory: str, keyword: str, extension: str = "xlsx") -> List[str]:
    """
    Finds all files in a directory that contain a specific keyword and have the specified extension.

    :param directory: Directory path to search for files
    :param keyword: Keyword to match in filenames
    :param extension: File extension to match (default is 'xlsx')
    :return: List of matching filenames

    Example usage:
    >>> files = findExcelFilesByMatch("./data", "sales")
    >>> print(files)

    >>> csvFiles = findExcelFilesByMatch("./data", "sales", "csv")
    >>> print(csvFiles)
    """
    matchedFiles = []
    pattern = f"*{keyword}*.{extension}"
    
    for fileName in os.listdir(directory):
        if fnmatch.fnmatch(fileName, pattern):
            matchedFiles.append(fileName)

    return matchedFiles

def getCurrentTimestamp() -> str:
    """
    Returns the current date and time formatted as 'YYYY-MM-DDTHH-MM-SS'.

    :return: A string representing the current timestamp
    Example usage:
    >>> timestamp = getCurrentTimestamp()
    >>> print(timestamp)
    """
    return datetime.now().strftime("%Y-%m-%dT%H-%M-%S")

def createFolder(baseFolderName: str, basePath: str = ".", addTimestamp: bool = True) -> Optional[str]:
    """
    Creates a folder with the base name, optionally followed by the current timestamp.

    :param baseFolderName: Base name for the folder
    :param basePath: Directory where the folder will be created (default is the current directory)
    :param addTimestamp: Whether to append the current timestamp to the folder name (default is True)
    :return: Full path of the created folder or None if creation fails

    Example usage:
    >>> folderPath = createFolder("report")
    >>> print(folderPath)

    >>> folderPathNoTimestamp = createFolder("report", addTimestamp=False)
    >>> print(folderPathNoTimestamp)
    """
    try:
        fullFolderName = baseFolderName
        if addTimestamp:
            fullFolderName += f"_{getCurrentTimestamp()}"
        
        fullPath = os.path.join(basePath, fullFolderName)

        os.makedirs(fullPath, exist_ok=True)
        print(f"Folder created: {fullPath}")
        return fullPath
    except Exception as e:
        print(f"Error creating the folder: {e}")
        return None

def moveFilesToFolder(fileList: List[str], destinationFolder: str) -> None:
    """
    Moves a list of files to a destination folder.

    :param fileList: List of file paths to move
    :param destinationFolder: Path of the destination folder
    :return: None

    Example usage:
    >>> moveFilesToFolder(["./file1.txt", "./file2.txt"], "./backup")
    """
    try:
        # Create the destination folder if it doesn't exist
        if not os.path.exists(destinationFolder):
            createdFolder = createFolder(destinationFolder, basePath=".", addTimestamp=False)
            if createdFolder:
                destinationFolder = createdFolder
            else:
                raise Exception(f"Failed to create the destination folder: {destinationFolder}")

        for filePath in fileList:
            if os.path.isfile(filePath):
                destinationPath = os.path.join(destinationFolder, os.path.basename(filePath))
                shutil.move(filePath, destinationPath)
                print(f"File moved: {filePath} -> {destinationPath}")
            else:
                print(f"File does not exist: {filePath}")
    except Exception as e:
        print(f"Error moving files: {e}")

def validateFiles(fileList: List[str]) -> bool:
    """
    Validates that a list of file paths does not contain empty values.

    :param fileList: List of file paths
    :return: True if all files are valid, False if there are missing files

    Example usage:
    >>> isValid = validateFiles(["file1.xlsx", "file2.xlsx", ""])
    >>> print(isValid)
    """
    missingFiles = [filePath for filePath in fileList if filePath.strip() == ""]

    if missingFiles:
        print("Error: Missing required files for the report process.")
        return False

    print("All files are valid.")
    return True
