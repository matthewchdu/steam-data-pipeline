# seed = populating an empty database
# metadata = the data about steam games

import gzip
import ast
import json
import psycopg2
from pathlib import Path

def get_db_connection():
    #Connects to a Postgre database `steam_metadata`
    connection = psycopg2.connect("host=localhost " \
    "port=5432 " \
    "dbname=steam_metadata " \
    "user=root " \
    "password=root")
    return connection


def create_target_table(connection):
    #Sets up an empty table for game data to be put in
    cursor = connection.cursor()

    cursor.execute("DROP TABLE IF EXISTS games")

    #Secondary keys `app_name` and `release date` for quick queries
    #Payload also included as it is raw data to be transformed later
    #release_date is set to TEXT to avoid errors, keeping it simple
    cursor.execute("CREATE TABLE games (id serial PRIMARY KEY, " \
    "app_name TEXT, " \
    "release_date TEXT, " \
    "payload JSONB)")

    connection.commit()
    cursor.close()
    return connection

def parse_and_load_data(connection,file_path):
    cursor = connection.cursor()
    count = 0

    #Query with placeholder variables to be filled in during the for-loop
    query = """
    INSERT INTO games (app_name, release_date, payload)
    VALUES (%s, %s, %s)
    """    

    with gzip.open(file_path,'rt',encoding='utf-8') as file:
        for line in file:
            if not line.strip():
                #If empty
                continue
            else:
                #Turns the record into a dictionary
                # e.g. u'publisher' as a column, it's Python 2 so cannot use json.loads()
                temp = ast.literal_eval(line)

                #Gathers the data
                app_name = temp.get('app_name')
                release_date = temp.get('release_date')
                payload = json.dumps(temp)

                #Executes query
                cursor.execute(query, (app_name, release_date, payload))

                #Tracks the amount of rows executed, after 5000 rows, it commits it to the database
                count+=1
                if count % 5000 == 0:
                    connection.commit()
                    print(f"Committed {count} rows")

        #Final commit
        connection.commit()
        cursor.close()
        print(f"Committed {count} rows")

def main():
    #Gets the path
    file_path = Path(__file__).resolve().parent.parent / "raw" / "steam_games.json.gz"

    connection = get_db_connection()
    create_target_table(connection)
    parse_and_load_data(connection,file_path)
    connection.close()

if __name__ == "__main__":
    main()


# Example row
# {u'publisher': u'Kotoshiro',
#  u'genres': [u'Action', u'Casual', u'Indie', u'Simulation', u'Strategy'], 
# u'app_name': u'Lost Summoner Kitty', u'title': u'Lost Summoner Kitty', 
# u'url': u'http://store.steampowered.com/app/761140/Lost_Summoner_Kitty/', 
# u'release_date': u'2018-01-04', 
# u'tags': [u'Strategy'