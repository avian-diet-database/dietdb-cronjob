import requests
import csv
import mysql.connector
import os
import filecmp
from mysql.connector import errorcode
from datetime import datetime
from config import *


def printLog(data):
    print("[LOG] " + str(data))


def printError(data):
    print("[ERROR] " + str(data))


def printElapsedTime():
    elapsed_time = datetime.now() - start
    printLog("Elapsed time: " + str(elapsed_time.total_seconds()) + " seconds")


filename = 'dietdatabase.txt'
start = datetime.now()

printLog(start.isoformat() + " - Updating avian_diet table")

source_file_info = requests.get(source_data_info)
json_response = source_file_info.json()
time = json_response[0]["commit"]["author"]["date"]
src_file_last_commit_time = datetime.strptime(time, '%Y-%m-%dT%H:%M:%SZ')
days_since = (start - src_file_last_commit_time).days

if days_since > 7:
    printLog("File from github was last updated " + days_since + " days ago, no changes since last update")
    printElapsedTime()
    exit()

try:
    r = requests.get(source_data_url)
except Exception as e:
    printError(e)
    printElapsedTime()
    exit(1)

if not r.ok:
    printError("Source data url returned error: " + source_data_url)
    printElapsedTime()
    exit(1)

with open(filename, 'wb') as f:
    f.write(r.content)

try:
    conn = mysql.connector.connect(host=db_host,
                                   user=db_user,
                                   password=db_pass,
                                   database=db_name)
except mysql.connector.Error as err:
    if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
        printError("Something is wrong with your user name or password")
    elif err.errno == errorcode.ER_BAD_DB_ERROR:
        printError("Database '" + db_name + "' not found")
    else:
        printError(err)
    os.remove(filename)
    printElapsedTime()
    exit(1)

cursor = conn.cursor()

# Add data to a new table to back up old table later
try:
    cursor.execute("DROP TABLE IF EXISTS avian_diet_new")
    cursor.execute("CREATE TABLE avian_diet_new LIKE avian_diet")
except mysql.connector.Error as err:
    print(err.msg)
    cursor.close()
    conn.close()
    os.remove(filename)
    printElapsedTime()
    exit(1)

with open(filename, newline='', encoding='cp1252') as new_table:
    reader = csv.reader(new_table, delimiter='\t')
    # Skip header
    next(reader)
    i = 0
    for row in reader:
        #print(data)
        for index, value in enumerate(row):
            if value in ['NA', '']:
                row[index] = None
            if index in [13, 14, 15, 16, 35, 36]:
                try:
                    if row[index] is not None:
                        if index == 35 and row[index] == "unspecified":
                            row[index] = None
                        else:
                            row[index] = int(row[index].split(".", 1)[0])
                except Exception as e:
                    printError("Failed parsing row data as integer on row:")
                    printError(row)
                    printError(e)
                    cursor.close()
                    conn.close()
                    os.remove(filename)
                    printElapsedTime()
                    exit(1)
        try:
            cursor.execute(
                """
            INSERT INTO avian_diet_new
            (common_name, scientific_name, subspecies, family,
            taxonomy,
            longitude_dd, latitude_dd, altitude_min_m, altitude_mean_m, altitude_max_m,
            location_region, location_specific, habitat_type,
            observation_month_begin, observation_year_begin, observation_month_end, observation_year_end, observation_season,
            analysis_number,
            prey_kingdom, prey_phylum, prey_class, prey_order, prey_suborder, prey_family, prey_genus, prey_scientific_name,
            inclusive_prey_taxon,
            prey_name_ITIS_ID, prey_name_status,
            prey_stage,
            prey_part,
            prey_common_name,
            fraction_diet, diet_type,
            item_sample_size, bird_sample_size,
            sites, study_type, notes, entered_by, source)
            VALUES
            (%s, %s, %s, %s,
            %s,
            %s, %s, %s, %s, %s,
            %s, %s, %s,
            %s, %s, %s, %s, %s,
            %s,
            %s, %s, %s, %s, %s, %s, %s, %s,
            %s,
            %s, %s,
            %s,
            %s,
            %s,
            %s, %s,
            %s, %s,
            %s, %s, %s, %s, %s)
            """, tuple(row))
        except mysql.connector.Error as err:
            printError("Failed inserting row:")
            printError(row)
            printError(err.msg)
            cursor.close()
            conn.close()
            os.remove(filename)
            printElapsedTime()
            exit(1)

try:
    # Removes previous backup
    cursor.execute("DROP TABLE IF EXISTS avian_diet_old")
    cursor.execute("RENAME TABLE avian_diet TO avian_diet_old")
    cursor.execute("RENAME TABLE avian_diet_new TO avian_diet")
except mysql.connector.Error as err:
    printError("Failed renaming tables")
    printError(err.msg)
    cursor.close()
    conn.close()
    os.remove(filename)
    printElapsedTime()
    exit(1)

try:
    last_updated_time = start.strftime("%B %d, %Y %H:%M:%S UTC")
    cursor.execute("INSERT table_history(table_name,last_updated) VALUES ('avian_diet', %s) ON DUPLICATE KEY UPDATE last_updated=%s", (last_updated_time, last_updated_time))
except mysql.connector.Error as err:
    printError("Failed updating last updated timestamp for avian_diet in table_history")
    printError(err.msg)
    cursor.close()
    conn.close()
    os.remove(filename)
    printElapsedTime()
    exit(1)

conn.commit()
cursor.close()
conn.close()
os.remove(filename)
printLog("Successfully updated database")
printElapsedTime()
exit(0)
