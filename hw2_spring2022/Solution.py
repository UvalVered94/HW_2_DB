from typing import List
import Utility.DBConnector as Connector
from Utility.Status import Status
from Utility.Exceptions import DatabaseException
from Business.File import File
from Business.RAM import RAM
from Business.Disk import Disk
from Utility.DBConnector import ResultSet
from psycopg2 import sql


def createTables():
    conn = None
    try:
        conn = Connector.DBConnector()

        conn.execute("CREATE TABLE Files(file_id INTEGER NOT NULL PRIMARY KEY,"
                          "file_type TEXT NOT NULL,"
                          "size_needed INTEGER NOT NULL,"
                          "CHECK (file_id > 0),"
                          "CHECK (size_needed >= 0))")

        conn.execute("CREATE TABLE Disks(disk_id INTEGER NOT NULL PRIMARY KEY,"
                          "manufacturing_company TEXT NOT NULL,"
                          "speed INTEGER NOT NULL,"
                          "free_space INTEGER NOT NULL,"
                          "cost_per_byte INTEGER NOT NULL,"
                          "CHECK (disk_id > 0),"
                          "CHECK (speed > 0),"
                          "CHECK (cost_per_byte > 0),"
                          "CHECK (free_space >= 0))")

        conn.execute("CREATE TABLE Rams(ram_id INTEGER NOT NULL PRIMARY KEY,"
                          "size INTEGER NOT NULL,"
                          "company TEXT NOT NULL,"
                          "CHECK (ram_id > 0),"
                          "CHECK (size > 0))")

        ################################################################## TO MODIFY
        '''conn.execute("CREATE TABLE file_belong_to_disk(ram_id INTEGER NOT NULL PRIMARY KEY,"
                          "size INTEGER NOT NULL,"
                          "company TEXT NOT NULL,"
                          "CHECK (ram_id > 0),"
                          "CHECK (size > 0))")'''
        ##################################################################### TO MODIFY
        '''conn.execute("CREATE TABLE ram_attached_to_disk(file_id INTEGER NOT NULL PRIMARY KEY,"
                     "disk_id INTEGER NOT NULL"
                          "size INTEGER NOT NULL,"
                          "company TEXT NOT NULL,"
                          "CHECK (ram_id > 0),"
                          "CHECK (size > 0))")'''
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
        conn.commit()
        conn.close()  # whatever happens, at the end, ending connection to server
        return Status.OK

def clearTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        tables_to_clear = ["Files", "Disks", "Rams"]
        for table in tables_to_clear:
            conn.execute("TRUNCATE TABLE " + table) #  maybe should use DELETE FROM instead of TRUNCATE TABLE
    except Exception as e:
        return Status.ERROR
    finally:
        conn.commit()
        conn.close()
        return Status.OK


def dropTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        tables_to_drop = ["Files", "Disks", "Rams"]
        '''views_to_drop = [...]'''
        '''for view in views_to_drop:
            conn.execute("DROP VIEW IF EXISTS " + view " CASCADE")'''
        for table in tables_to_drop:
            conn.execute("DROP TABLE IF EXISTS " + table + " CASCADE")
    except Exception as e:
        conn.close()
        return Status.ERROR
    finally:
        conn.commit()
        conn.close()
    pass


def addFile(file: File) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        add_file_query = sql.SQL("INSERT INTO Files(file_id, file_type, size_needed)"
                        "VALUES({file_id}, {file_type}, {size_needed})") \
            .format(file_id=sql.Literal(file.getFileID()), file_type=sql.Literal(file.getType()),\
                    size_needed=sql.Literal(file.getSize()))
        rows_effected, _ = conn.execute(add_file_query)
    except DatabaseException.ConnectionInvalid as e:
            conn.close()
            return Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION as e:
            conn.close()
            return Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION as e:
            conn.close()
            return Status.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION as e:
            conn.close()
            return Status.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return Status.BAD_PARAMS
    except DatabaseException.UNKNOWN_ERROR as e:
            conn.close()
            return Status.ERROR
    except Exception as e:
        conn.close()
        return Status.ERROR
    finally:
        conn.commit()
        conn.close()
        return Status.OK


def getFileByID(fileID: int) -> File:
    conn = None
    try:
        rows_effected, result = 0, ResultSet()
        conn = Connector.DBConnector()
        get_file_query = sql.SQL("SELECT * FROM Files WHERE file_id = {id_of_file}") \
            .format(id_of_file=sql.Literal(fileID))
        rows_effected, result = conn.execute(get_file_query)
        # the rows effected var is the number of rows received by the SELECT func
        if rows_effected != 0:
            return File(result[0]["file_id"], result[0]["file_type"], result[0]["size_needed"])
        else:
            return File.badFile()
    except DatabaseException.ConnectionInvalid:
        conn.close()
        return Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        conn.close()
        return Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        conn.close()
        return Status.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION:
        conn.close()
        return Status.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return Status.BAD_PARAMS
    except DatabaseException.UNKNOWN_ERROR:
        conn.close()
        return Status.ERROR
    except Exception as e:
        conn.close()
        return Status.ERROR
    finally:
        conn.commit()
        conn.close()


def deleteFile(file: File) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        delete_file_query = sql.SQL("DELETE FROM Files WHERE file_id = {id_of_file}") \
        .format(id_of_file=sql.Literal(file.getFileID()))
        rows_effected, _ = conn.execute(delete_file_query)
        conn.commit()
        if rows_effected == 0:
            return Status.NOT_EXISTS

    except DatabaseException.ConnectionInvalid:
        conn.close()
        return Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        conn.close()
        return Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        conn.close()
        return Status.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION:
        conn.close()
        return Status.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return Status.BAD_PARAMS
    except DatabaseException.UNKNOWN_ERROR:
        conn.close()
        return Status.ERROR
    except Exception as e:
        conn.close()
        return Status.ERROR
    finally:
        conn.close()
        return Status.OK


def addDisk(disk: Disk) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        add_disk_query = sql.SQL("INSERT INTO Disks(disk_id, manufacturing_company, speed, free_space, cost_per_byte)"
                        "VALUES({disk_id}, {manufacturing_company}, {speed}, {free_space}, {cost_per_byte})") \
            .format(disk_id=sql.Literal(disk.getDiskID()), manufacturing_company=sql.Literal(disk.getCompany()), \
                    speed=sql.Literal(disk.getSpeed()), free_space=sql.Literal(disk.getFreeSpace()), \
                    cost_per_byte=sql.Literal(disk.getCost()))
        rows_effected, _ = conn.execute(add_disk_query)
    except DatabaseException.ConnectionInvalid:
        conn.close()
        return Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        conn.close()
        return Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        conn.close()
        return Status.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION:
        conn.close()
        return Status.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return Status.BAD_PARAMS
    except DatabaseException.UNKNOWN_ERROR:
        conn.close()
        return Status.ERROR
    except Exception as e:
        conn.close()
        return Status.ERROR
    finally:
        conn.commit()
        conn.close()
        return Status.OK


def getDiskByID(diskID: int) -> Disk:
    conn = None
    try:
        rows_effected, result = 0, ResultSet()
        conn = Connector.DBConnector()
        get_disk_query = sql.SQL("SELECT * FROM Disks WHERE disk_id = {id_of_file}") \
            .format(id_of_file=sql.Literal(diskID))
        rows_effected, result = conn.execute(get_disk_query)
        # the rows effected var is the number of rows received by the SELECT func
        if rows_effected != 0:
            return Disk(result[0]["disk_id"], result[0]["manufacturing_company"], result[0]["speed"], \
                        result[0]["free_space"], result[0]["cost_per_byte"])
        else:
            return Disk.badDisk()
    except DatabaseException.ConnectionInvalid:
        conn.close()
        return Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        conn.close()
        return Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        conn.close()
        return Status.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION:
        conn.close()
        return Status.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return Status.BAD_PARAMS
    except DatabaseException.UNKNOWN_ERROR:
        conn.close()
        return Status.ERROR
    except Exception as e:
        conn.close()
        return Status.ERROR
    finally:
        conn.commit()
        conn.close()


def deleteDisk(diskID: int) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        delete_disk_query = sql.SQL("DELETE FROM Disks WHERE disk_id = {id_of_disk}") \
            .format(id_of_disk=sql.Literal(diskID))
        rows_effected, _ = conn.execute(delete_disk_query)
        if rows_effected == 0:
            return Status.NOT_EXISTS

    except DatabaseException.ConnectionInvalid:
        conn.close()
        return Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        conn.close()
        return Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        conn.close()
        return Status.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION:
        conn.close()
        return Status.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return Status.BAD_PARAMS
    except DatabaseException.UNKNOWN_ERROR:
        conn.close()
        return Status.ERROR
    except Exception as e:
        conn.close()
        return Status.ERROR
    finally:
        conn.commit()
        conn.close()
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

if __name__ == '__main__':
    dropTables()
    createTables()
    road = 1  # put 0 for Files table testing, 1 for Disks, 2 for Rams
    if road == 0:
        new_file0 = File(123456, 'JPEG', 1096)
        print(new_file0.getFileID())
        print(new_file0.getSize())
        print(new_file0.getType())
        addFile(new_file0)
        new_file1 = File(11111111, 'MATAN GAY', 8096)
        print(new_file1.getFileID())
        print(new_file1.getSize())
        print(new_file1.getType())
        addFile(new_file1)
        returned_file = getFileByID(11111111)
        print("returned file TYPE is: ", returned_file.getType())
        # deleteFile(returned_file)
    if road == 1:
        new_disk0 = Disk(101, "Foxcon", 200, 2056, 1)
        new_disk1 = Disk(201, "Foxcon", 300, 3056, 2)
        addDisk(new_disk0)
        addDisk(new_disk1)
        returned_disk = getDiskByID(201)
        print("returned disk free space is: ", returned_disk.getFreeSpace())
        deleteDisk(201)


