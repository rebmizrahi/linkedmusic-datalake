import psycopg2

# Database connection parameters
db_params = {
    'dbname': 'simssadb_test',
    'user': 'postgres',
    'password': 'debug',
    'host': 'localhost'
}

# List of table names
table_names = ['contribution_musical_work', 'musical_work']#, 'part']
left_join_id = ['contributed_to_work_id']
right_join_id = ['id']

# Construct the join conditions
join_conditions = " ".join([f"FULL OUTER JOIN {table} ON {prev_table}.{left_id} = {table}.{right_id}" \
                            for prev_table, table, left_id, right_id in zip(table_names[:-1], table_names[1:], left_join_id[0:], right_join_id[0:])])

# query = f"""
#     SELECT *
#     FROM {table_names[0]}
#     {join_conditions};
# """

query = """
    SELECT
        contribution_musical_work.id AS contribution_id,
        contribution_musical_work.role AS contributor_role,
        contribution_musical_work.certainty_of_attribution AS contributor_certainty_of_attribution,
        musical_work.id AS musical_work_id,
        musical_work.variant_titles AS musical_work_variant_titles,
        musical_work.sacred_or_secular AS sacred_or_secular
    FROM
        contribution_musical_work
    FULL OUTER JOIN
        musical_work ON contribution_musical_work.contributed_to_work_id = musical_work.id;
"""


# Connect to the database
conn = psycopg2.connect(**db_params)
cur = conn.cursor()

# Execute the query
cur.execute(query)

# Retrieve data
result = cur.fetchall()

# Export data to CSV
with open('output.csv', 'w') as csv_file:
    # Write header
    header = [desc[0] for desc in cur.description]
    csv_file.write(','.join(header) + '\n')

    # Write data rows
    for row in result:
        csv_file.write(','.join(map(str, row)) + '\n')

# Clean up
cur.close()
conn.close()


