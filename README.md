# pyLoader
## Introduction
Example of leveraging Python futures and PostgreSQL COPY command to bulk load Bungie.net API data.
### Background
The video game studio Bungie has created an API through which developers can access data for the video game Destiny 2. The API has both public and private endpoints that can pull data (public and private endpoints) and manage inventory of characters (private endpoints). Bungie has imposed a rate limit on the API as would be expected to avoid abuse. Currently, Bungie throttles requests that exceed 25 requests per second over a rolling ten second period.

Avoiding the rate limit is pretty easy using single-threaded requests where the network latency of the request-response round trip is incurred for every request. Unfortunately, not all endpoints are created alike. Some return a tremendous amount of data that requires additional processing time to parse and, in this scenario, load to a database. As a result, single-threaded API processing needs to be replaced with multi-threaded API processing using Python futures which will be explained later.

Database latency also presents a challenge to overall performance as well. Executing a single database INSERT followed by a COMMIT for each record to be inserted from the API response can easily extend overall processing time. Fortunately, PostgreSQL provides a COPY command for fast copying of file data directly into a database table. The extremely popular Python library psycopg2 has an implemenation of the COPY command that supports the copying of file-like data directly to a table which will be explained in detail later.
## Sample Results
The following results are based on the Bungie API endpoint GetAggregateActivityStats.
### Single-threaded processing
The number of total requests has been limited to ten characters, or ten API requests, for single-threaded processing.

```
API Requests: 10
API Execution: 4.83s
Database Inserts: 12848
Database Execution: 22.34s
```

### Multi-threaded processing
The number of total requests has not been limited for multi-threaded processing.

```
Requests 551
API Execution: 34.78s
Database Loading Execution: 38.84s
Inserts: 672768
```

A comparison of performance is pretty telling. The single-threaded throughput for the API averaged ~0.48s per request, while the multi-threaded processing averaged ~0.06s per request. The traditional database insert averaged ~0.06s per INSERT while the COPY command averaged ~0.00006s per insert.
# Details
## API
### Single-threaded processing
A typical model for pullling data would be to gather all the attributes needed for a particular endpoint, loop through the attributes, and request data. As mentioned earlier, each request-response round trip is completed before another request-response round trip is initiated. The process flow might look like this:

[insert process flow]

### Multi-threaded processing
The solution to streamlining API performance is to minimize the network latency without violating the API rate limit. Multi-threaded processing can be accomplished by using the Python concurrent library. In particular, the module futures supports asynchronous execution of callables.

In the single-threaded model, 25 API requests would incur the network latency 25 times. In the multi-threaded model, 25 API requests execute simultaneously so the network latency is incurred simultaneously as well. The number 25 is used intentionally because it is the rate limit of the Bungie API.

The multi-threaded process chunks the requests into batches of 25 and submits a chunk at a time for processing. The new process flow might look like this:

[insert process flow] 
## Database
### Standard database INSERT
A standard database insert requires a database engine to parse the DML statement to make sure the syntax is correct and the data to be inserted complies with keys, etc. built on the table into which data is being added. Also, inserts occur within transactions so a database COMMIT is required to essentially commit the data to the table in the database once the insert completes successfully.

Assuming a table structure as follows:
[insert data model]

A snippet of Python using psycopg2 to insert data follows:
```python
# open database connection
pg = psycopg2.connect()

# psycopg2 INSERT statement
statInsert = "INSERT INTO stats.t_aggregate_activity_stats(group_id, clan_id, member_id, character_id, activity_hash, stat_id, stat) VALUES (%s, %s, %s, %s, %s, %s, %s)"

# psycopg2 CURSOR
pg_cursor = pg.cursor() # create the cursor
pg_cursor.execute(statInsert, (field1, field2, field3, field4, field5, field6, field7)) # execute the DML

# psycopg2 COMMIT
pg.commit()

# close database connection
pg.close()
```
The process flow using standard database inserts might look as follows:

[insert process flow]

Databases are very efficient at processing INSERT statements and even doing a succession of INSERT statements can be made to occur more quickly if only a single COMMIT is performed after all INSERT statements are executed. Unfortunately, INSERT statements don't scale well even when the COMMIT is delayed. The underlying database overhead of INSERT statements creates latency in the processing that can not be optimized.

###PostgreSQL COPY
The *COPY FROM* command essentially tells PostgreSQL to read data from a file and append it to a table within the database. The COPY command does not incur the overhead of a database INSERT statement because the operation is handled differently by the PostgreSQL database engine.

The popular Python library *psycopg2* has implemented the COPY command with a twist. psycopg2 supports the ability to load file-like data created within a Python script to the database. The format of the data within the file-like data is very prescriptive and much match the expected format of the table into which the data is being appended. Assuming the data to be loaded has the following format:

```json

```

A snippet of Python using psycopg2 to build and load the file-like data might look like this:
```python
for row in chunk:
    file_data += '\t'.join(str(value) for value in row) + '\n'
    total_inserts += 1

buffer = io.StringIO()
buffer.write(file_data)
buffer.seek(0)

pgCursor = db.cursor()
pgCursor.copy_from(buffer, 'stats.t_aggregate_activity_stats', sep='\t', columns=('group_id', 'clan_id', 'member_id', 'character_id', 'activity_hash', 'stat_id', 'stat'))

db.commit()

```
A couple of important things in the snippet to mention specifically. The statement
```python
file_data += '\t'.join(str(value) for value in row) + '\n'
```
converts a row of data into a tab-delimited(\t), Unix line feed (\n) terminated string which is then concatenated to a larger holding variable (file_data). The variable *file_data* is then written to a buffer. The buffer is then used as file-like data fed to PostgreSQL in the statement
```python
pgCursor.copy_from(buffer, 'stats.t_aggregate_activity_stats', sep='\t', columns=('group_id', 'clan_id', 'member_id', 'character_id', 'activity_hash', 'stat_id', 'stat'))
```
The important parameter in the *psycopg2.copy_from* method is *sep* which stands for separator. As you can see, the Unix tab delimiter (\t) used to format the data is also used by PostgreSQL to ingest the file-like data. The separator used to build the file-like data and ingest the data using the *COPY_FROM* method **MUST** be the same.

The new process flow might look something like this:

[insert process flow]
##Fine Print
The devil is always in the details and this process is no different. The unique identifiers for platform, clan, account, and character data maintained by Bungie does not necessarily match the unique identifiers within a custom database. As a result, continuity must be maintained throughout the processing to make sure the final insert is successful.

The API endpoint GetAggregateActivityStats requires three parameters: Platform (PlayStation, Xbox, or Battle.net), Account, and Character. The query used to request the necessary parameters for getting stats returns each character with the Bungie identifiers and local database identifiers as a JSON object using the PostgreSQL *JSONB_BUILD_OBJECT* function. The return from the query is structured as followed:
```json
{
  "clan_id": 2373515,
  "group_id": 1,
  "bungie_id": 10071956,
  "clan_name": "Iron Orange Moon",
  "member_id": 174,
  "class_hash": 3655393761,
  "destiny_id": 4611686018444414000,
  "group_name": "Iron Orange",
  "bungie_name": "dad2cl3",
  "character_id": 2305843009264369700,
  "destiny_name": "dad2cl3",
  "bungie_membership_type": 254,
  "destiny_membership_type": 2
}
```
Each individual record returned from the database is appended with the URL that needs to be processed to get the stats. Once added, the record looks as follows:
```json
{
  "clan_id": 2373515,
  "group_id": 1,
  "bungie_id": 10071956,
  "clan_name": "Iron Orange Moon",
  "member_id": 174,
  "class_hash": 3655393761,
  "destiny_id": 4611686018444414000,
  "group_name": "Iron Orange",
  "bungie_name": "dad2cl3",
  "character_id": 2305843009264369700,
  "destiny_name": "dad2cl3",
  "bungie_membership_type": 254,
  "destiny_membership_type": 2,
  "requestUrl": "https://www.bungie.net/Platform/Destiny2/2/Account/4611686018438308034/Character/2305843009267620413/Stats/AggregateActivityStats/"
}
```
Lastly, the actual stats returned are also appended to the original JSON returned from the database. The JSON data (shortened considerably) now looks as follows:
```json
{
  "clan_id": 2373515,
  "group_id": 1,
  "bungie_id": 10071956,
  "clan_name": "Iron Orange Moon",
  "member_id": 174,
  "class_hash": 3655393761,
  "destiny_id": 4611686018444414000,
  "group_name": "Iron Orange",
  "bungie_name": "dad2cl3",
  "character_id": 2305843009264369700,
  "destiny_name": "dad2cl3",
  "bungie_membership_type": 254,
  "destiny_membership_type": 2,
  "requestUrl": "https://www.bungie.net/Platform/Destiny2/2/Account/4611686018438308034/Character/2305843009267620413/Stats/AggregateActivityStats/",
  "stats": {
    "Response": {
      "activities": [
        {
          "activityHash": 2183066491,
          "values": {
            "fastestCompletionMsForActivity": {
              "statId": "fastestCompletionMsForActivity",
              "basic": {
                "value": 13300,
                "displayValue": "0:13.300"
              },
              "activityId": "316045713"
            }
          }
        }
      ]
    },
    "ErrorCode": 1,
    "ThrottleSeconds": 0,
    "ErrorStatus": "Success",
    "Message": "Ok",
    "MessageData": {}
  }
}
```
Finally, an array of arrays is built for the insert of each individual stat per character. A single array within the array looks as follows:
```Python
[1, 802118, 10, 2305843009267620413, 3631476566, 'activityCompletions', '{"statId": "activityCompletions", "basic": {"value": 0.0, "displayValue": "0"}}']
```
The array of arrays is turned into file-like data and written to the database in user-defined chunks.
