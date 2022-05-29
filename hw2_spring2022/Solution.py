from decimal import DivisionByZero
from msilib.schema import RemoveFile
from typing import List
import Utility.DBConnector as Connector
from Utility.Status import Status
from Utility.Exceptions import DatabaseException
from Business.File import File
from Business.RAM import RAM
from Business.Disk import Disk
from Utility.DBConnector import ResultSet
from psycopg2 import sql
from psycopg2.errors import DivisionByZero

from hw2_spring2022.Utility.DBConnector import DBConnector

def createTables():
    conn = None
    try:
        conn = Connector.DBConnector()

        conn.execute("BEGIN; CREATE TABLE Files(file_id INTEGER NOT NULL PRIMARY KEY,"
                          "file_type TEXT NOT NULL,"
                          "size_needed INTEGER NOT NULL,"
                          "CHECK (file_id > 0),"
                          "CHECK (size_needed >= 0));"
                     "CREATE TABLE Disks(disk_id INTEGER NOT NULL PRIMARY KEY,"
                          "manufacturing_company TEXT NOT NULL,"
                          "speed INTEGER NOT NULL,"
                          "free_space INTEGER NOT NULL,"
                          "cost_per_byte INTEGER NOT NULL,"
                          "CHECK (disk_id > 0),"
                          "CHECK (speed > 0),"
                          "CHECK (cost_per_byte > 0),"
                          "CHECK (free_space >= 0));"
                     "CREATE TABLE Rams(ram_id INTEGER NOT NULL PRIMARY KEY,"
                          "ram_size INTEGER NOT NULL,"
                          "company TEXT NOT NULL,"
                          "CHECK (ram_id > 0),"
                          "CHECK (ram_size > 0));"
                     "CREATE TABLE Files_inside_Disks(file_id INTEGER NOT NULL,"
                         "disk_id INTEGER NOT NULL,"
                         "FOREIGN KEY (file_id) REFERENCES Files(file_id) ON DELETE CASCADE,"
                         "FOREIGN KEY (disk_id) REFERENCES Disks(disk_id) ON DELETE CASCADE,"
                         "CONSTRAINT pk_FinD PRIMARY KEY (file_id, disk_id));"# the name of the primary key is FinD
                    "CREATE TABLE Rams_inside_Disks(ram_id INTEGER NOT NULL,"
                         "disk_id INTEGER NOT NULL,"
                         "FOREIGN KEY (ram_id) REFERENCES Rams(ram_id) ON DELETE CASCADE,"
                         "FOREIGN KEY (disk_id) REFERENCES Disks(disk_id) ON DELETE CASCADE,"
                         "CONSTRAINT pk_RinD PRIMARY KEY (ram_id, disk_id));"  # the name of the primary key is RinD
                    "CREATE VIEW DisksNRams_info AS  "
                         "SELECT disk_id, COALESCE (SUM(ram_size),0) as entire_disk_ram"
                         " FROM RAMS R INNER JOIN Rams_inside_Disks RiD"
                         " ON R.ram_id = RiD.ram_id"
                         " GROUP BY disk_id "
                         " UNION"
                         " SELECT disk_id, 0 FROM Disks D WHERE disk_id NOT IN (SELECT disk_id from Rams_inside_Disks);"
                    "CREATE VIEW FilesNDisks_info AS  "
                         "SELECT disk_id, COALESCE(SUM(F.size_needed),0) as size_occupied, (SELECT free_space FROM Disks WHERE disk_id = FiD.disk_id) as free_space, COALESCE(COUNT(FiD.file_id),0) as num_of_files"
                         " FROM Files F INNER JOIN Files_inside_Disks FiD"
                         " ON FiD.file_id = F.file_id"
                         " GROUP BY disk_id "
                         " UNION"
                         " SELECT disk_id, 0, D.free_space, 0 FROM Disks D WHERE disk_id NOT IN (SELECT disk_id from Files_inside_Disks);"
                     "COMMIT;")

    except Exception as e:
        conn.rollback()
    else:
        conn.commit()
    finally:
        if conn is not None:
            conn.close()
        return

# TODO: make it into 1 transaction
def clearTables():
    conn = None
    try:
        conn = Connector.DBConnector()
        tables_to_clear = ["Files", "Disks", "Rams"] #, "Files_inside_Disks", "Rams_inside_Disks"]
        for table in tables_to_clear:
            conn.execute("TRUNCATE TABLE " + table + " CASCADE")
    except Exception as e:
        print("WARNING!! ERROR RAISED IN VOID FUNCTION clearTables!")
        print(e)
    else:
        conn.commit()
    finally:
        if conn is not None:
            conn.close()
        return

# TODO: make it into 1 transaction
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
        print("WARNING!! ERROR RAISED IN VOID FUNCTION dropTables!")
        print(e)
    else:
        conn.commit()
    finally:
        if conn is not None:
            conn.close()
        return


def addFile(file: File) -> Status:
    status = Status.OK
    conn = None
    try:
        conn = Connector.DBConnector()
        add_file_query = sql.SQL("INSERT INTO Files(file_id, file_type, size_needed)" \
                        "VALUES({file_id}, {file_type}, {size_needed})") \
            .format(file_id=sql.Literal(file.getFileID()), file_type=sql.Literal(file.getType()),\
                    size_needed=sql.Literal(file.getSize()))
        rows_affected, _ = conn.execute(add_file_query)
    except DatabaseException.ConnectionInvalid:
        status = Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        status = Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        status = Status.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION:
        status = Status.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        status = Status.BAD_PARAMS
    except DatabaseException.UNKNOWN_ERROR:
        status = Status.ERROR
    except Exception as e: # all other
        status = Status.ERROR
        print("WARNING! CATCHING ANY EXCEPTION TYPE IN AddFile!")
    else:
        conn.commit()
    finally:
        if conn is not None:
            conn.close()
        return status


def getFileByID(fileID: int) -> File:
    file = File.badFile()
    conn = None
    try:
        rows_affected, result = 0, ResultSet()
        conn = Connector.DBConnector()
        get_file_query = sql.SQL("SELECT * FROM Files WHERE file_id = {id_of_file}") \
            .format(id_of_file=sql.Literal(fileID))
        rows_affected, result = conn.execute(get_file_query)
        # the rows effected var is the number of rows received by the SELECT func
        if rows_affected != 0:
            file = File(result[0]["file_id"], result[0]["file_type"], result[0]["size_needed"])
    except Exception as e:
        print("WARNING! CATCHING ANY EXCEPTION TYPE IN GetFileByID!")
    else:
        conn.commit()
    finally:
        if conn is not None:
            conn.close()
        return file


def deleteFile(file: File) -> Status:
    status = Status.OK
    conn = None
    try:
        conn = Connector.DBConnector()
        delete_file_query = sql.SQL("DELETE FROM Files WHERE file_id = {id_of_file}") \
        .format(id_of_file=sql.Literal(file.getFileID()))
        _, _ = conn.execute(delete_file_query)
    except DatabaseException.ConnectionInvalid:
        status = Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        status = Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        status = Status.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION:
        status = Status.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        status = Status.BAD_PARAMS
    except DatabaseException.UNKNOWN_ERROR:
        status = Status.ERROR
    except Exception as e:
        print("WARNING! CATCHING ANY EXCEPTION TYPE IN DeleteFile!")
        status = Status.ERROR
    else:
        conn.commit()
    finally:
        if conn is not None:
            conn.close()
        return status


def addDisk(disk: Disk) -> Status:
    status = Status.OK
    conn = None
    try:
        conn = Connector.DBConnector()
        add_disk_query = sql.SQL("INSERT INTO Disks(disk_id, manufacturing_company, speed, free_space, cost_per_byte)"
                        "VALUES({disk_id}, {manufacturing_company}, {speed}, {free_space}, {cost_per_byte})") \
            .format(disk_id=sql.Literal(disk.getDiskID()), manufacturing_company=sql.Literal(disk.getCompany()), \
                    speed=sql.Literal(disk.getSpeed()), free_space=sql.Literal(disk.getFreeSpace()), \
                    cost_per_byte=sql.Literal(disk.getCost()))
        rows_affected, _ = conn.execute(add_disk_query)
    except DatabaseException.ConnectionInvalid:
        status = Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        status = Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        status = Status.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION:
        status = Status.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        status = Status.BAD_PARAMS
    except DatabaseException.UNKNOWN_ERROR:
        status = Status.ERROR
    except Exception as e:
        print("WARNING! CATCHING ANY EXCEPTION TYPE IN AddDisk!")
        status = Status.ERROR
    else:
        conn.commit()
    finally:
        if conn is not None:
            conn.close()
        return status


def getDiskByID(diskID: int) -> Disk:
    disk = Disk.badDisk()
    conn = None
    try:
        rows_affected, result = 0, ResultSet()
        conn = Connector.DBConnector()
        get_disk_query = sql.SQL\
            (" \
            SELECT * FROM \
                ((SELECT D.disk_id, D.manufacturing_company, D.speed, D.cost_per_byte FROM Disks D WHERE disk_id = {disk_id}) AA \
                CROSS JOIN \
                (SELECT free_space from FilesNDisks_info WHERE disk_id = {disk_id}) BB )\
            DISK_INFO").format(disk_id=sql.Literal(diskID))
        rows_affected, result = conn.execute(get_disk_query)
        # the rows effected var is the number of rows received by the SELECT func
        if rows_affected != 0:
            disk = Disk(result[0]["disk_id"], result[0]["manufacturing_company"], result[0]["speed"], \
                        result[0]["free_space"], result[0]["cost_per_byte"])
    except Exception as e:
        print("WARNING! CATCHING ANY EXCEPTION TYPE IN getDiskByID!")
        print(str(e))
    else:
        conn.commit()
    finally:
        if conn is not None:
            conn.close()
        return disk


def deleteDisk(diskID: int) -> Status:
    status = Status.OK
    conn = None
    try:
        conn = Connector.DBConnector()
        delete_disk_query = sql.SQL("DELETE FROM Disks WHERE disk_id = {id_of_disk}") \
            .format(id_of_disk=sql.Literal(diskID))
        rows_affected, _ = conn.execute(delete_disk_query)
        if rows_affected == 0:
            status = Status.NOT_EXISTS
    except DatabaseException.ConnectionInvalid:
        status = Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        status = Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        status = Status.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION:
        status = Status.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        status = Status.BAD_PARAMS
    except DatabaseException.UNKNOWN_ERROR:
        status = Status.ERROR
    except Exception as e:
        print("WARNING! CATCHING ANY TYPE OF EXCEPTION IN deleteDisk")
        status = Status.ERROR
    else:
        conn.commit()
    finally:
        if conn is not None:
            conn.close()
        return status


def addRAM(ram: RAM) -> Status:
    status = Status.OK
    conn = None
    try:
        conn = Connector.DBConnector()
        add_ram_query = sql.SQL("INSERT INTO Rams(ram_id, ram_size, company)"
                                 "VALUES({ram_id}, {ram_size}, {company})") \
            .format(ram_id=sql.Literal(ram.getRamID()), ram_size=sql.Literal(ram.getSize()), \
                    company=sql.Literal(ram.getCompany()))
        rows_affected, _ = conn.execute(add_ram_query)
    except DatabaseException.ConnectionInvalid:
        status = Status.ERROR
    except DatabaseException.NOT_NULL_VIOLATION:
        status = Status.BAD_PARAMS
    except DatabaseException.UNIQUE_VIOLATION:
        status = Status.ALREADY_EXISTS
    except DatabaseException.CHECK_VIOLATION:
        status = Status.BAD_PARAMS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        status = Status.BAD_PARAMS
    except DatabaseException.UNKNOWN_ERROR:
        status = Status.ERROR
    except Exception as e:
        print("WARNING! CATCHING ANY TYPE OF EXCEPTION IN deleteDisk")
        status = Status.ERROR
    else:
        conn.commit()
    finally:
        if conn is not None:
            conn.close()
        return status


def getRAMByID(ramID: int) -> RAM:
    conn = None
    ram = RAM.badRAM()
    try:
        rows_affected, result = 0, ResultSet()
        conn = Connector.DBConnector()
        get_ram_query = sql.SQL("SELECT * FROM Rams WHERE ram_id = {id_of_ram}") \
            .format(id_of_ram=sql.Literal(ramID))
        rows_affected, result = conn.execute(get_ram_query)
        # the rows effected var is the number of rows received by the SELECT func
        if rows_affected != 0:  # should be 1 as ramID is a key
            ram = RAM(result[0]["ram_id"], result[0]["company"], result[0]["ram_size"])
    except Exception as e:
        pass
    else:
        conn.commit()
    finally:
        if conn is not None:
            conn.close()
        return ram


def deleteRAM(ramID: int) -> Status:
    conn = None
    status = Status.OK
    try:
        conn = Connector.DBConnector()
        delete_ram_query = sql.SQL("DELETE FROM Rams WHERE ram_id = {id_of_ram}") \
            .format(id_of_ram=sql.Literal(ramID))
        rows_affected, _ = conn.execute(delete_ram_query)
        if rows_affected == 0:
            status = Status.NOT_EXISTS
    except (DatabaseException.ConnectionInvalid, DatabaseException.UNKNOWN_ERROR, Exception):
        status = Status.ERROR
    else:
        conn.commit()
    finally:
        if conn is not None:
            conn.close()
        return status


def addDiskAndFile(disk: Disk, file: File) -> Status:
    conn = None
    status = Status.OK
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
        rows_affected, _ = conn.execute(add_diskNfile_query)
    except DatabaseException.UNIQUE_VIOLATION:
        status = Status.ALREADY_EXISTS
        conn.rollback()
    except (DatabaseException.ConnectionInvalid, DatabaseException.UNKNOWN_ERROR, Exception):
        status = Status.ERROR
        conn.rollback()
    else:
        conn.commit()
    finally:
        if conn is not None:
            conn.close()
        return status


def addFileToDisk(file: File, diskID: int) -> Status:
    conn = None
    status = Status.OK
    try:
        conn = Connector.DBConnector()
        add_file_to_disk_query = sql.SQL("BEGIN; \
                                                INSERT INTO Files_inside_Disks (file_id, disk_id) VALUES({file_id}, {disk_id}); \
                                                UPDATE Disks SET free_space = free_space - {size_needed}; \
                                        COMMIT;").format(file_id=sql.Literal(file.getFileID()), disk_id=sql.Literal(diskID), size_needed=sql.Literal(file.getSize()))
        conn.execute(add_file_to_disk_query)
        # cannot use rows_affected, query_result from a transaction
    except (DatabaseException.ConnectionInvalid, DatabaseException.UNKNOWN_ERROR):
        status = Status.ERROR
    except DatabaseException.UNIQUE_VIOLATION:
        status = Status.ALREADY_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        status = Status.NOT_EXISTS
    except DatabaseException.CHECK_VIOLATION:
        status = Status.BAD_PARAMS
    except Exception as e:
        status = Status.ERROR
    else:
        conn.commit()
    finally:
        if status != Status.OK:
            conn.rollback()
        if conn is not None:
            conn.close()
        return status


def removeFileFromDisk(file: File, diskID: int) -> Status:
    conn = None
    status = Status.OK
    try:
        conn = Connector.DBConnector()
        remove_file_from_disk_query = sql.SQL("BEGIN; \
                                                DELETE FROM Files_inside_Disks WHERE file_id = {file_id} and disk_id = {disk_id}; \
                                                UPDATE Disks SET free_space = free_space + {size_needed}; \
                                        COMMIT;").format(file_id=sql.Literal(file.getFileID()), disk_id=sql.Literal(diskID), size_needed=sql.Literal(file.getSize()))
                                        
        rows_affected, _ = conn.execute(remove_file_from_disk_query)
    except Exception as e:
        print(e)
        status = Status.ERROR
    else:
        conn.commit()
    finally:
        if conn is not None:
            conn.close()
        return status


def addRAMToDisk(ramID: int, diskID: int) -> Status:
    conn = None
    status = Status.OK
    try:
        conn = Connector.DBConnector()
        add_ram_to_disk_query = sql.SQL("INSERT INTO Rams_inside_Disks(ram_id, disk_id) VALUES({ram_id}, {disk_id})").format(
            ram_id=sql.Literal(ramID),
            disk_id=sql.Literal(diskID))
        rows_affected, _ = conn.execute(add_ram_to_disk_query)
        if rows_affected == 0:
            status = Status.NOT_EXISTS
    except DatabaseException.FOREIGN_KEY_VIOLATION:
        status = Status.NOT_EXISTS
    except DatabaseException.UNIQUE_VIOLATION:
        status = Status.ALREADY_EXISTS
    except Exception as e:
        print(e)
        status = Status.ERROR
    else:
        conn.commit()
    finally:
        if conn is not None:
            conn.close()
        return status


def removeRAMFromDisk(ramID: int, diskID: int) -> Status:
    conn = None
    status = Status.OK
    try:
        conn = Connector.DBConnector()
        query_string = "DELETE FROM Rams_inside_Disks WHERE ram_id = " + str(ramID) + " and disk_id = " + str(diskID)
        remove_ram_from_disk_query = sql.SQL(query_string)
        rows_affected, _ = conn.execute(remove_ram_from_disk_query)
        if rows_affected == 0:
            status = Status.NOT_EXISTS
    except Exception as e:
        print(e)
        status = Status.ERROR
    else:
        conn.commit()
    finally:
        if conn is not None:
            conn.close()
        return status


def averageFileSizeOnDisk(diskID: int) -> float:
    conn = None
    result = ResultSet()
    rows_affected = 0
    avg_result = 0
    try:
        conn = Connector.DBConnector()
        average_query = sql.SQL("SELECT COALESCE((V.size_occupied / V.num_of_files),0) avg, V.num_of_files as nof FROM FilesNDisks_info V WHERE V.disk_id = {disk_id}").format(
            disk_id=sql.Literal(diskID))
        rows_affected, result = conn.execute(average_query)
        if rows_affected != 0: # No disk with this ID
            avg_result = result[0]["avg"]
    except DivisionByZero as e:
        avg_result = 0
    except Exception as e:
        avg_result = -1
    else:
        conn.commit()
    finally:
        if conn is not None:
            conn.close()
        return avg_result


def diskTotalRAM(diskID: int) -> int:
    ram_result = 0
    conn = None
    try:
        conn = Connector.DBConnector()
        query_string = "SELECT entire_disk_ram FROM DisksNRams_info WHERE disk_id = " + str(diskID) # should be 1 or 0 lines
        disk_tot_ram_query = sql.SQL(query_string)
        rows_affected, query_result = conn.execute(disk_tot_ram_query)
        if rows_affected == 1:
            ram_result = int(query_result[0]["entire_disk_ram"])
    except:
        ram_result = -1
    else:
        conn.commit()
    finally:
        if conn is not None:
            conn.close()
        return ram_result


def getCostForType(type: str) -> int:
    cost_result = 0
    conn = None
    try:
        conn = Connector.DBConnector()
        cost_for_type_query = sql.SQL("SELECT COALESCE(SUM(price_to_disk), 0) AS cost_per_disk FROM \
        ( \
            SELECT SUM(FND.size_needed)*D.cost_per_byte AS price_to_disk \
            FROM (\
                ((SELECT * FROM Files WHERE file_type = {file_type}) F \
                INNER JOIN Files_inside_Disks FD ON F.file_id = FD.file_id) FND \
                INNER JOIN Disks D ON FND.disk_id = D.disk_id \
                ) \
            GROUP BY D.disk_id, D.cost_per_byte \
        ) SUMS_VECTOR").format(file_type=sql.Literal(type))
        rows_affected, query_result = conn.execute(cost_for_type_query)
        if rows_affected == 1: # should ALWAYS be 1 because of the coalesce
            cost_result = query_result[0]["cost_per_disk"]
    except Exception as e:
        cost_result = -1
    else:
        conn.commit()
    finally:
        if conn is not None:
            conn.close()
        return cost_result


def getFilesCanBeAddedToDisk(diskID: int) -> List[int]:
    conn = None
    answer = []
    rows_affected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        can_add_query = sql.SQL("SELECT file_id FROM Files F WHERE size_needed <= (SELECT free_space FROM FilesNDisks_info "
         "WHERE disk_id = {disk_id}) "
         "ORDER BY file_id " 
         "DESC LIMIT 5").format(disk_id=sql.Literal(diskID))
        rows_affected, result = conn.execute(can_add_query)
        if rows_affected != 0: # the list should not be empty
            for file in range(rows_affected):
                answer.append(result[file]["file_id"])
    except Exception as e:
        print(e)
        pass    
    else:
        conn.commit()
    finally:
        if conn is not None:
            conn.close()
        return answer


def getFilesCanBeAddedToDiskAndRAM(diskID: int) -> List[int]:
    conn = None
    answer = []
    rows_affected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        can_add_query = sql.SQL("SELECT file_id FROM Files F WHERE size_needed <= (SELECT free_space FROM FilesNDisks_info "
         "WHERE disk_id = {disk_id}) and size_needed <= (SELECT entire_disk_ram FROM DisksNRams_info "
         "WHERE disk_id = {disk_id})"
         "ORDER BY file_id " 
         "ASC LIMIT 5").format(disk_id=sql.Literal(diskID))
        rows_affected, result = conn.execute(can_add_query)
        if rows_affected != 0: # the list should not be empty
            for file in range(rows_affected):
                answer.append(result[file]["file_id"])
    except Exception as e:
        print(e)
        pass    
    else:
        conn.commit()
    finally:
        if conn is not None:
            conn.close()
        return answer

# TODO: check what happens if there is no such disk, if disk has no RAM
def isCompanyExclusive(diskID: int) -> bool:
    result = False
    conn = None
    try:
        conn = Connector.DBConnector()
        exclusive_query = sql.SQL("\
        SELECT ram_result FROM \
        ( \
            SELECT COUNT(*) AS ram_result FROM DisksNRams_info \
            WHERE \
                disk_id = {disk_id} and \
                entire_disk_ram =   (\
                                        SELECT COALESCE(SUM(ram_size),0) FROM Rams WHERE \
                                                                                company = (SELECT manufacturing_company FROM Disks WHERE disk_id = {disk_id})\
                                                                                and ram_id IN (SELECT ram_id FROM Rams_inside_Disks WHERE disk_id = {disk_id})\
                                    ) \
        ) \
        RAM_RESULT").format(disk_id=sql.Literal(diskID))
        rows_affected, query_result = conn.execute(exclusive_query)
        if rows_affected != 0:
            if int(query_result[0]["ram_result"]) == 1:
                result = True
    except Exception as e:
        pass
    else:
        conn.commit()
    finally:
        if conn is not None:
            conn.close()
        return result

def getConflictingDisks() -> List[int]:
    conn = None
    answer =[]
    rows_affected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        
        conflict_query = sql.SQL("SELECT FiD.disk_id FROM Files_inside_Disks as FiD WHERE (SELECT COUNT(FD.disk_id) FROM Files_inside_Disks as FD WHERE FD.file_id = FiD.file_id) > 1 ORDER BY FiD.disk_id")
        rows_affected, result = conn.execute(conflict_query)
        for disk in range(rows_affected):
            answer.append(result[disk]["disk_id"])
    except Exception as e:
        print(e)
    else:
        conn.commit()
    finally:
        if conn is not None:
            conn.close()
        return answer
    
def mostAvailableDisks() -> List[int]:
    conn = None
    result = []
    try:
        conn = Connector.DBConnector()
        most_disks_query = sql.SQL("\
        SELECT FFND.disk_id AS disk_id, Disks.speed AS disk_speed, COUNT(FFND.disk_id) AS files_possible FROM \
        (SELECT disk_id FROM Files CROSS JOIN FilesNDisks_info WHERE Files.size_needed <= FilesNDisks_info.free_space) FFND INNER JOIN Disks ON FFND.disk_id = Disks.disk_id \
        GROUP BY FFND.disk_id, Disks.speed \
        ORDER BY files_possible DESC, disk_speed DESC, disk_id ASC \
        LIMIT 5 \
        ")
        rows_affected, query_result = conn.execute(most_disks_query)
        if rows_affected!= 0:
            for index in range(rows_affected):
                result.append(query_result[index]["disk_id"])
    except Exception as e:
        pass # nothing to do if there is an error, return an empty result
    else:
        conn.commit()
    finally:
        if conn is not None:
            conn.close()
        return result

def getCloseFiles(fileID: int) -> List[int]:
    conn = None
    answer =[]
    rows_affected, result = 0, ResultSet()
    try:
        conn = Connector.DBConnector()
        num_of_intersections = "SELECT COUNT(INTER.disk_id) FROM "\
            "(SELECT F1.disk_id FROM Files_inside_Disks F1 WHERE F1.file_id = {file_id}" \
            "INTERSECT SELECT F2.disk_id FROM Files_inside_Disks F2 WHERE F2.file_id = F.file_id " \
             ") INTER"
        num_of_disks = "SELECT COUNT(F3.disk_id) " \
                       "FROM Files_inside_Disks F3 " \
                       "WHERE F3.file_id = {file_id} "
        conflict_query = sql.SQL("SELECT F.file_id FROM Files F WHERE (" + num_of_intersections + ") >= 0.5*(" \
                                 + num_of_disks + ") AND NOT (F.file_id={file_id}) ORDER BY F.file_id").format(file_id=sql.Literal(fileID))
        rows_affected, result = conn.execute(conflict_query)
        for file in range(rows_affected):
            answer.append(result[file]["file_id"])
    except Exception as e:
        print(e)
    else:
        conn.commit()
    finally:
        if conn is not None:
            conn.close()
        return answer


def lane_six():
    new_disk0 = Disk(3, "Foxcon", 50, 10000, 10000)
    new_file = File(1,"jpeg",1000)
    print("add disk: " + str(addDisk(new_disk0)))
    print("add file: " + str(addFile(new_file)))
    print("add file to disk: " + str(addFileToDisk(new_file, 3)))
    print("remove file: " + str(deleteFile(new_file)))
    #print("isExclusive: " + str(isCompanyExclusive(3)))

if __name__ == '__main__':
    dropTables()
    createTables()
    road = 6 # put 0 for Files table testing, 1 for Disks, 2 for Rams, 3 for disk & file
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
        new_disk1 = Disk(1111, "WD", 350, 5500, 10)
        new_file1 = File(111, 'JPEG', 1000)
        addDiskAndFile(new_disk1, new_file1)
        disk = getDiskByID(1111)
        print(disk.getFreeSpace())
        addFileToDisk(new_file1, 1111)
        disk = getDiskByID(1111)
        print(disk.getFreeSpace())
    if road == 4:
        new_ram0 = RAM(31259, "Samsung", 16096)
        addRAM(new_ram0)
        new_disk0 = Disk(1111, "Foxconn", 200, 2056, 1)
        addDisk(new_disk0)
        addRAMToDisk(3159, 1111)
        print("before remove")
        removeRAMFromDisk(5656, 1111)
        print("after remove")
    if road == 5:
        print("Im game")
    if road == 6:
        lane_six()