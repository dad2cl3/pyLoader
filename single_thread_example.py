import json, psycopg2, requests, time

with open('config.json', 'r') as configFile:
    config = json.load(configFile)

api_config = config['API']
db_config = config['Database']
sql_config = config['SQL']

def handler (event, context):
    # connect to database
    pg = psycopg2.connect(host=db_config['host'], port=db_config['port'], database=db_config['database'], user=db_config['user'], password=db_config['password'])
    # get characters
    pg_cursor = pg.cursor()
    pg_cursor.execute(sql_config['characterSelect'])
    query_results = pg_cursor.fetchall()

    characters = query_results

    insert_counter = 0
    db_duration = 0
    api_duration = 0
    # get aggregate stats
    for character in characters:
        character = character[0]
        url = api_config['url'].format(character['destiny_membership_type'], character['destiny_id'], character['character_id'])
        print(url)

        headers = {
            'X-API-Key': api_config['xApiKey']
        }
        api_start_time = time.time()
        response = requests.get(url, headers=headers)
        api_end_time = time.time()
        api_duration += (api_end_time - api_start_time)

        if response.status_code == 200:
            # load aggregate stats
            data = response.json()
            activities = data['Response']['activities']

            for activity in activities:
                activity_hash = activity['activityHash']
                stats = activity['values']
                for stat in stats:
                    stat_id = stats[stat]['statId']
                    stat = stats[stat]

                    db_start_time = time.time()
                    pg_cursor.execute(sql_config['statInsert'], (
                        character['group_id'],
                        character['clan_id'],
                        character['member_id'],
                        character['character_id'],
                        activity_hash,
                        stat_id,
                        json.dumps(stat)
                    ))

                    pg.commit()
                    db_end_time = time.time()
                    db_duration += (db_end_time - db_start_time)
                    insert_counter += 1

    print('API Execution: {0:.2f}s'.format(api_duration))
    print('Database Inserts: {0}'.format(insert_counter))
    print('Database Execution: {0:.2f}s'.format(db_duration))
    # close database connection

    pg.close()