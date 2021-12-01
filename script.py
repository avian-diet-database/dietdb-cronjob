import requests
import csv
import mysql.connector
import os
import traceback
import filecmp
import pandas
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
approvedTableFile = 'approvedtable.txt'
columnNames = ['common_name', 'scientific_name', 'subspecies', 'family', 'taxonomy', 'longitude_dd', 'latitude_dd', 'altitude_min_m', 'altitude_max_m', 'altitude_mean_m', 'location_region', 'location_specific', 'habitat_type', 'observation_month_begin', 'observation_month_end', 'observation_year_begin', 'observation_year_end', 'observation_season', 'analysis_number', 'prey_kingdom', 'prey_phylum', 'prey_class', 'prey_order', 'prey_suborder', 'prey_family', 'prey_genus', 'prey_scientific_name', 'inclusive_prey_taxon', 'prey_name_ITIS_ID', 'prey_name_status', 'prey_stage', 'prey_part', 'prey_common_name', 'fraction_diet', 'diet_type', 'item_sample_size', 'bird_sample_size', 'sites', 'study_type', 'notes', 'entered_by', 'source', 'doi', 'sex', 'age_class', 'within_study_data_source', 'table_fig_number', 'title', 'lastname_author', 'source_year', 'journal']
start = datetime.now()

printLog(start.isoformat() + " - Updating avian_diet table")

source_file_info = requests.get(source_data_info)
json_response = source_file_info.json()
time = json_response[0]["commit"]["author"]["date"]
src_file_last_commit_time = datetime.strptime(time, '%Y-%m-%dT%H:%M:%SZ')
days_since = (start - src_file_last_commit_time).days

if days_since > 7:
    printLog("File from github was last updated " + str(days_since) + " days ago, no changes since last update")
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

# Add data to a new table to back up old table later..this step happends now because Creating/Deleting implicitly commits
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

print("Fetching from DB where state is approved")
#Fetch avian_diet_pending where state = "approved"

cursor.execute("SELECT * FROM approved_diet_view")
approved_table_results = cursor.fetchall()

#Update all "approved" records to approved/processed"
cursor.execute("UPDATE avian_diet_pending SET state = 'approved/processed' WHERE unique_id > 0 AND state = 'approved'")

with open(approvedTableFile,'w',newline='',encoding='cp1252') as f:
    csvwriter = csv.writer(f,delimiter='\t', quoting=csv.QUOTE_MINIMAL)
    for x in approved_table_results:
        csvwriter.writerow(x)

print("Filling nulls with string NA for flat file formatting")
#fill nulls with 'NA'
with open(approvedTableFile,'rb') as f:
    dataframe = pandas.read_csv(f, delimiter= '\t', names=columnNames, dtype=str)

with open(approvedTableFile,'wb') as f:
    dataframe.to_csv(f,sep='\t',index=False,na_rep='NA',header=False)

print("Appending new approved data to flat file")
#append to local flat file
with open(filename,'ab') as f:
    with open(approvedTableFile,'rb') as w:
        f.write(w.read())


print("Begin inserting rows to DB" + str(datetime.now()))
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
                    new_table.close()
                    os.remove(filename)
                    os.remove(approvedTableFile)
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
            sites, study_type, notes, entered_by, source,
            doi, sex, age_class, within_study_data_source, table_fig_number, title, lastname_author, source_year, journal)
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
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, tuple(row))
        except mysql.connector.Error as err:
            printError("Failed inserting row:")
            printError(row)
            printError(err.msg)
            cursor.close()
            conn.close()
            new_table.close()
            os.remove(filename)
            os.remove(approvedTableFile)
            printElapsedTime()
            exit(1)

print("done inserting rows to db" + str(datetime.now()))

print("begin updating repo")
#Update github repo


token = {'Authorization': 'Bearer %s' %personal_access_token}
path = 'AvianDietDatabase.txt'
commit_message = 'cronjob update %s' %str(datetime.now())

try:
    r = requests.get('https://api.github.com/repos/hurlbertlab/dietdatabase/git/refs/heads/master')
    #   print(r.json())
    old_commit_sha = r.json()["object"]["sha"]
    # print('old_commit_sha' + old_commit_sha)
except:
    printError("Failed getting latest commit sha from master reference")
    traceback.print_exc()
    cursor.close()
    conn.close()
    os.remove(filename)
    os.remove(approvedTableFile)
    printElapsedTime()
    exit(1)

try:
    
    r2 = requests.get('https://api.github.com/repos/hurlbertlab/dietdatabase/git/commits/%s'%old_commit_sha)
    # print(r2.json())
    old_tree_sha = r2.json()["tree"]["sha"]
    # print('old_tree_sha' + old_tree_sha)
except:
    printError("Failed getting latest tree sha from latest commit")
    traceback.print_exc()
    cursor.close()
    conn.close()
    os.remove(filename)
    os.remove(approvedTableFile)
    printElapsedTime()
    exit(1)

local_appended_opener = open(filename,'r')
content = local_appended_opener.read()

try:
    new_tree_input = {
    'tree': [{
        'path': '%s'%path,
        'mode': '100644',
        'type': 'blob',
        'content':'%s'%content
        }],
    'base_tree': '%s'%old_tree_sha
    }
    r3 = requests.post('https://api.github.com/repos/hurlbertlab/dietdatabase/git/trees', json=new_tree_input, headers=token)
    # print(r3.json())
    new_tree_sha = r3.json()["sha"]
    print('new_tree_sha' + new_tree_sha)
except:
    printError("Failed creating new tree")
    traceback.print_exc()
    cursor.close()
    conn.close()
    local_appended_opener.close()
    os.remove(filename)
    os.remove(approvedTableFile)
    printElapsedTime()
    exit(1)

local_appended_opener.close()

try:
    new_commit_input = {
        'message': 'Cronjob update ' + str(start),
        'tree': '%s'%new_tree_sha,
        'parents': ['%s'%old_commit_sha]
    }
    r4 = requests.post('https://api.github.com/repos/hurlbertlab/dietdatabase/git/commits', json=new_commit_input, headers=token)
    # print(r4.json())
    new_commit_sha = r4.json()["sha"]
    # print('new_commit_sha' + new_commit_sha)

except:
    printError("Failed creating new commit")
    traceback.print_exc()
    cursor.close()
    conn.close()
    os.remove(filename)
    os.remove(approvedTableFile)
    printElapsedTime()
    exit(1)

try:
    update_reference_input = {
        'sha': '%s'%new_commit_sha
    }
    r5 = requests.patch('https://api.github.com/repos/hurlbertlab/dietdatabase/git/refs/heads/master', json=update_reference_input,headers=token)
    # print(r5.json())
except:
    printError("Failed updating reference to new commit")
    print('new_commit_sha that failed to get referenced' + new_commit_sha)
    traceback.print_exc()
    cursor.close()
    conn.close()
    os.remove(filename)
    os.remove(approvedTableFile)
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
    os.remove(approvedTableFile)
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
    os.remove(approvedTableFile)
    printElapsedTime()
    exit(1)

conn.commit()
cursor.close()
conn.close()
os.remove(filename)
os.remove(approvedTableFile)
printLog("Successfully updated database")
printElapsedTime()
exit(0)
