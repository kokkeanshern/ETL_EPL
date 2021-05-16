import pymysql

# Establish connection and create cursor object.
def create_connection(host, user, pwd, db):
    connection = pymysql.connect(host = host,
                                 user = user,
                                 password = pwd,
                                 db = db)
    cursor = connection.cursor()
    return cursor, connection


# Insert rows into table.
def insert(df, cursor, connection):
    # comma separated list of columns.
    cols = ",".join(df.columns.tolist())

    print("Starting INSERTS for ",df.name)
    for row in df.itertuples(index = False):
        vals = ",".join(['"{}"'.format(str(i)) for i in list(row)])
        sql_insert = "INSERT INTO " + df.name + " (" + cols + ") VALUES (" + vals + ");"
        cursor.execute(sql_insert)

        connection.commit()
    print("Completed INSERTS for ",df.name)


