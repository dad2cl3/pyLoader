from concurrent import futures

import psycopg2, requests, json, time, io

with open('config.json', 'r') as configFile:
    config = json.load(configFile)

api_config = config['API']
db_config = config['Database']
sql_config = config['SQL']


def execute_ddl(db, ddl):
    print('Executing DDL statement on database...')

    start = time.time()

    pg_cursor = db.cursor()
    pg_cursor.execute(ddl)
    db.commit()

    end = time.time()
    ddl_duration = end - start
    print('Database Execution: {0:.2f}s'.format(ddl_duration))


def get_characters(db):
    print('Getting characters...')

    character_sql = sql_config['characterSelect']

    pg_cursor = db.cursor()
    pg_cursor.execute(character_sql)

    characters = pg_cursor.fetchall()

    characters = list(map(list, characters))

    return characters


def build_requests(characters):
    print('Building requests...')

    requests = []

    for character in characters:
        #print(character[0])
        url = [api_config['url'].format(character[0]['destiny_membership_type'], character[0]['destiny_id'], character[0]['character_id'])]

        character[0]['requestUrl'] = url
        #print(json.dumps(character))

        requests.append(character)

    return requests


def process_requests(characters):
    print('Processing requests...')

    stats = []

    character_count = len(characters)
    print('Requests {0}'.format(character_count))
    chunk_size = 25
    start = 0

    api_start_time = time.time()

    for start in range(0, character_count, chunk_size):
        end = start + chunk_size - 1
        if end > character_count:
            end = character_count - 1

        #print('({0},{1})'.format(start, end))

        print('Processing chunk {0} - {1}'.format(str(start), str(end)))
        chunk = end - start + 1

        with futures.ThreadPoolExecutor(chunk) as executor:
            future_to_url = {executor.submit(get_stats, character): character for character in characters[start:(end + 1)]}

        for response in futures.as_completed(future_to_url):
            stats.append(response.result())

    api_end_time = time.time()
    api_duration = api_end_time - api_start_time

    print('API Execution: {0:.2f}s'.format(api_duration))
    return stats


def get_stats(character):
    # print('Getting stats...')

    x_api_key = api_config['xApiKey']
    #print(character[0]['requestUrl'][0])

    response = requests.get(character[0]['requestUrl'][0], headers={'X-API-Key': x_api_key})
    stats = response.text
    #print(stats)
    character[0]['stats'] = json.loads(stats)

    #print(json.dumps(character))

    # print(json.loads(stats)['ErrorStatus'])

    return character


def build_inserts(character_stats):
    print('Building database inserts...')

    inserts = []

    for character in character_stats:
        activities = character[0]['stats']['Response']['activities']

        for activity in activities:
            activityHash = [activity['activityHash']]

            stats = activity['values']
            for stat in stats:
                data = stats[stat]
                #print(character[0])
                insert = [character[0]['group_id'], character[0]['clan_id'], character[0]['member_id'], character[0]['character_id']] + activityHash + [stat] + [json.dumps(data)]
                #print(insert)
                inserts.append(insert)
    return inserts


def load_data(db, data):

    insert_count = len(data)
    print(insert_count)
    chunk_size = 10000

    if chunk_size > insert_count:
        chunk_size = insert_count

    start = 0
    total_inserts = 0

    db_start_time = time.time()

    print(insert_count)

    for start in range(0, insert_count, chunk_size):
        end = start + chunk_size - 1
        if end > insert_count:
            end = insert_count - 1

        #if (i % chunkSize == 0 and i > 0) or i == (insert_count - 1):
        print('Processing chunk {0} - {1}'.format(str(start), str(end)))

        chunk = data[start:(end+1)]

        file_data = ''

        for row in chunk:
            #print(row)
            file_data += '\t'.join(str(value) for value in row) + '\n'
            #print(file_data)
            total_inserts += 1

        buffer = io.StringIO()
        buffer.write(file_data)
        buffer.seek(0)

        pg_cursor = db.cursor()
        pg_cursor.copy_from(buffer, 'stats.t_aggregate_activity_stats', sep='\t', columns=('group_id', 'clan_id', 'member_id', 'character_id', 'activity_hash', 'stat_id', 'stat'))

        db.commit()

            #start = i + 1

    db_end_time = time.time()
    db_duration = db_end_time - db_start_time
    print('Database Loading Execution: {0:.2f}s'.format(db_duration))

    return total_inserts


def handler(event, context):
    # Open database connection
    #pg = pg8000.connect(host=dbConfig['host'], port=dbConfig['port'], database=dbConfig['database'], user=dbConfig['user'], password=dbConfig['password'])
    pg = psycopg2.connect(host=db_config['host'], port=db_config['port'], database=db_config['database'], user=db_config['user'], password=db_config['password'])

    # Truncate table
    execute_ddl(pg, sql_config['truncateActivity'])

    # Retrieve characters
    characters = get_characters(pg)

    # Build requests
    characters = build_requests(characters)

    # Process requests
    character_stats = process_requests(characters)

    # Build inserts
    inserts = build_inserts(character_stats)

    # Load data
    counts = load_data(pg, inserts)
    print('Inserts: {0}'.format(counts))

    # Post load activities
    # Analyze base table after loading
    execute_ddl(pg, sql_config['analyzeActivityTable'])
    # Refresh materialized view
    execute_ddl(pg, sql_config['refreshActivity'])
    # Analyze materialized view after refresh
    execute_ddl(pg, sql_config['analyzeActivityView'])

    pg.close()
