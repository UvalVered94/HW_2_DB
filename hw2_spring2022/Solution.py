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
                          "ram_size INTEGER NOT NULL,"
                          "company TEXT NOT NULL,"
                          "CHECK (ram_id > 0),"
                          "CHECK (ram_size > 0))")

        conn.execute("CREATE TABLE Files_inside_Disks(file_id INTEGER NOT NULL,"
                     " disk_id INTEGER NOT NULL,"
                     " FOREIGN KEY (file_id) REFERENCES Files(file_id) ON DELETE CASCADE,"
                     " FOREIGN KEY (disk_id) REFERENCES Disks(disk_id) ON DELETE CASCADE,"
                     " CONSTRAINT pk_FinD PRIMARY KEY (file_id, disk_id))")  # the name of the primary key is FinD

        conn.execute("CREATE TABLE Rams_inside_Disks(ram_id INTEGER NOT NULL,"
                     " disk_id INTEGER NOT NULL,"
                     " FOREIGN KEY (ram_id) REFERENCES Rams(ram_id) ON DELETE CASCADE,"
                     " FOREIGN KEY (disk_id) REFERENCES Disks(disk_id) ON DELETE CASCADE,"
                     " CONSTRAINT pk_RinD PRIMARY KEY (ram_id, disk_id))")  # the name of the primary key is RinD

        ''' a problem in roads 3 disappears if deleting two new tables!!!!!'''

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
        tables_to_clear = ["Files", "Disks", "Rams", "Files_inside_Disks", "Rams_inside_Disks"]
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
        tables_to_drop = ["Files", "Disks", "Rams", "Files_inside_Disks", "Rams_inside_Disks"]
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
    conn = None
    try:
        conn = Connector.DBConnector()
        add_ram_query = sql.SQL("INSERT INTO Rams(ram_id, ram_size, company)"
                                 "VALUES({ram_id}, {ram_size}, {company})") \
            .format(ram_id=sql.Literal(ram.getRamID()), ram_size=sql.Literal(ram.getSize()), \
                    company=sql.Literal(ram.getCompany()))
        rows_effected, _ = conn.execute(add_ram_query)
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


def getRAMByID(ramID: int) -> RAM:
    conn = None
    try:
        rows_effected, result = 0, ResultSet()
        conn = Connector.DBConnector()
        get_ram_query = sql.SQL("SELECT * FROM Rams WHERE ram_id = {id_of_ram}") \
            .format(id_of_ram=sql.Literal(ramID))
        rows_effected, result = conn.execute(get_ram_query)
        # the rows effected var is the number of rows received by the SELECT func
        if rows_effected != 0:
            return RAM(result[0]["ram_id"], result[0]["company"], result[0]["ram_size"])
        else:
            return RAM.badRAM()
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


def deleteRAM(ramID: int) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        delete_ram_query = sql.SQL("DELETE FROM Rams WHERE ram_id = {id_of_ram}") \
            .format(id_of_ram=sql.Literal(ramID))
        rows_effected, _ = conn.execute(delete_ram_query)
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


def addDiskAndFile(disk: Disk, file: File) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()

        add_diskNfile_query = sql.SQL("BEGIN;"
                                      "INSERT INTO Disks(disk_id, manufacturing_company, speed, free_space, cost_per_byte)"
                                 "VALUES({disk_id}, {manufacturing_company}, {speed}, {free_space}, {cost_per_byte});"
                                      "INSERT INTO Files(file_id, file_type, size_needed) VALUES({file_id}, {file_type}, {size_needed});"
                                      "COMMIT;").format(
                    disk_id=sql.Literal(disk.getDiskID()),
                    manufacturing_company=sql.Literal(disk.getCompany()),
                    speed=sql.Literal(disk.getSpeed()),
                    free_space=sql.Literal(disk.getFreeSpace()),
                    cost_per_byte=sql.Literal(disk.getCost()),
                    file_id=sql.Literal(file.getFileID()),
                    file_type=sql.Literal(file.getType()),
                    size_needed=sql.Literal(file.getSize()))
        rows_effected, _ = conn.execute(add_diskNfile_query)
    except DatabaseException.ConnectionInvalid:
        conn.close()
        return Status.ERROR
    except DatabaseException.UNIQUE_VIOLATION:
        conn.close()
        return Status.ALREADY_EXISTS
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


def addFileToDisk(file: File, diskID: int) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        add_file_to_disk_query = sql.SQL("INSERT INTO Files_inside_Disks(file_id, disk_id) VALUES({file_id}, {disk_id})").format(
            file_id=sql.Literal(file.getFileID()),
            disk_id=sql.Literal(diskID))
        rows_effected, _ = conn.execute(add_file_to_disk_query)
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
        return Status.NOT_EXISTS
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


def removeFileFromDisk(file: File, diskID: int) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        remove_file_from_disk_query = sql.SQL(
            "DELETE FROM Files_inside_Disks WHERE file_id = {file_id} and disk_id = {disk_id}").format(
            file_id=sql.Literal(file.getFileID()),
            disk_id=sql.Literal(diskID))
        rows_effected, _ = conn.execute(remove_file_from_disk_query)
    except DatabaseException.ConnectionInvalid:
        conn.close()
        return Status.ERROR
    except DatabaseException.FOREIGN_KEY_VIOLATION as e:
        return Status.NOT_EXISTS
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


def addRAMToDisk(ramID: int, diskID: int) -> Status:
    conn = None
    try:
        conn = Connector.DBConnector()
        add_ram_to_disk_query = sql.SQL("INSERT INTO Rams_inside_Disks(ram_id, disk_id) VALUES({ram_id}, {disk_id})").format(
            ram_id=sql.Literal(ramID),
            disk_id=sql.Literal(diskID))
        rows_effected, _ = conn.execute(add_ram_to_disk_query)
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
        return Status.NOT_EXISTS
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
    road = 3  # put 0 for Files table testing, 1 for Disks, 2 for Rams, 3 for disk & file
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
    if road == 2:
        new_ram0 = RAM(1234222, "NVIDIA", 4096)
        new_ram1 = RAM(3433, "Kingstone", 8096)
        addRAM(new_ram0)
        addRAM(new_ram1)
        returned_ram = getRAMByID(3433)
        print("returned ram size is: ", returned_ram.getSize())
        deleteRAM(1234222)
    if road == 3:
        new_disk0 = Disk(1111, "Foxconn", 200, 2056, 1)
        print(new_disk0.getCompany())
        new_file0 = File(8888, 'JPEG', 1096)
        print(new_file0.getType())
        addDiskAndFile(new_disk0, new_file0)
        addFileToDisk(new_file0, 1111)
        removeFileFromDisk(new_file0, 1111)
    if road == 4:
        new_file0 = File(8888, 'JPEG', 1096)
        removeFileFromDisk(new_file0, 1111)
