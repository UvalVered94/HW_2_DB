from typing import List
import Utility.DBConnector as Connector
from Utility.Status import Status
from Utility.Exceptions import DatabaseException
from Business.File import File
from Business.RAM import RAM
from Business.Disk import Disk
from psycopg2 import sql


def createTables():
    conn = None
    try:
        conn = Connector.DBConnector()

        conn.execute("CREATE TABLE Files(file_id INTEGER NOT NULL PRIMARY KEY,"
                          "type_id TEXT,"
                          "size_needed INTEGER UNIQUE,"
                          "CHECK (file_id > 0),"
                          "CHECK (size_needed >= 0))")
        conn.commit()
        conn.execute("CREATE TABLE Disks(disk_id INTEGER NOT NULL PRIMARY KEY,"
                          "manufacturing_company TEXT NOT NULL,"
                          "speed INTEGER NOT NULL,"
                          "free_space INTEGER NOT NULL,"
                          "cost_per_byte INTEGER NOT NULL,"
                          "CHECK (disk_id > 0),"
                          "CHECK (speed > 0),"
                          "CHECK (cost_per_byte > 0),"
                          "CHECK (free_space >= 0))")
        conn.commit()
        conn.execute("CREATE TABLE Rams(ram_id INTEGER NOT NULL PRIMARY KEY,"
                          "size INTEGER NOT NULL,"
                          "company TEXT NOT NULL,"
                          "CHECK (ram_id > 0),"
                          "CHECK (size > 0))")
        conn.commit()
        ################################################################## TO MODIFY
        conn.execute("CREATE TABLE file_belong_to_disk(ram_id INTEGER NOT NULL PRIMARY KEY,"
                          "size INTEGER NOT NULL,"
                          "company TEXT NOT NULL,"
                          "CHECK (ram_id > 0),"
                          "CHECK (size > 0))")
        conn.commit()
        ##################################################################### TO MODIFY
        conn.execute("CREATE TABLE ram_attached_to_disk(file_id INTEGER NOT NULL PRIMARY KEY,"
                     "disk_id INTEGER NOT NULL"
                          "size INTEGER NOT NULL,"
                          "company TEXT NOT NULL,"
                          "CHECK (ram_id > 0),"
                          "CHECK (size > 0))")
        conn.commit()
    except DatabaseException.ConnectionInvalid as e:
        return Status.ERROR

    except DatabaseException.NOT_NULL_VIOLATION as e:
        print(e)

    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        print(e)

    except DatabaseException.UNIQUE_VIOLATION as e:
        print(e)

    except DatabaseException.CHECK_VIOLATION as e:
        print(e)

    except DatabaseException.UNKNOWN_ERROR as e:
        print(e)

    except Exception as e:
        print(e)
    finally:
        conn.close()  # whatever happens, at the end, ending connection to server


def clearTables():
    conn = None
    try:
        conn = Connector.DBconnector()
        tables_to_clear = ["Files", "Disks", "Rams"]
        for table in tables_to_clear:
            conn.execute("TRUNCATE TABLE " + table) #  maybe should use DELETE FROM instead of TRUNCATE TABLE
    except Exception as e:
        return Status.ERROR
    finally:
        conn.close()


def dropTables():
    conn = None
    try:
        conn = Connector.DBconnector()
        tables_to_drop = ["Files", "Disks", "Rams"]
        '''views_to_drop = [...]'''
        '''for view in views_to_drop:
            conn.execute("DROP VIEW IF EXISTS " + view " CASCADE")'''
        for table in tables_to_drop:
            conn.execute("DROP TABLE IF EXISTS " + table + " CASCADE")
    except Exception as e:
        return Status.ERROR
    pass


def addFile(file: File) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        query = sql.SQL("INSERT INTO Users(id, name) VALUES({id}, {username})").format(id=sql.Literal(ID),
                                                                                       username=sql.Literal(name))
    return Status.OK


def getFileByID(fileID: int) -> File:
    return File()


def deleteFile(file: File) -> Status:
    return Status.OK


def addDisk(disk: Disk) -> Status:
    return Status.OK


def getDiskByID(diskID: int) -> Disk:
    return Disk()


def deleteDisk(diskID: int) -> Status:
    return Status.OK


def addRAM(ram: RAM) -> Status:
    return Status.OK


def getRAMByID(ramID: int) -> RAM:
    return RAM()


def deleteRAM(ramID: int) -> Status:
    return Status.OK


def addDiskAndFile(disk: Disk, file: File) -> Status:
    return Status.OK


def addFileToDisk(file: File, diskID: int) -> Status:
    return Status.OK


def removeFileFromDisk(file: File, diskID: int) -> Status:
    return Status.OK


def addRAMToDisk(ramID: int, diskID: int) -> Status:
    return Status.OK


def removeRAMFromDisk(ramID: int, diskID: int) -> Status:
    return Status.OK


def averageFileSizeOnDisk(diskID: int) -> float:
    return 0


def diskTotalRAM(diskID: int) -> int:
    return 0


def getCostForType(type: str) -> int:
    return 0


def getFilesCanBeAddedToDisk(diskID: int) -> List[int]:
    return []


def getFilesCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    return []


def isCompanyExclusive(diskID: int) -> bool:
    return True


def getConflictingDisks() -> List[int]:
    return []


def mostAvailableDisks() -> List[int]:
    return []


def getCloseFiles(fileID: int) -> List[int]:
    return []

'''print("monkey")
createTables()'''

