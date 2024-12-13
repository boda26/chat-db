import pymysql

def get_table_name(connection):
    try:
        # Create a cursor to interact with the database
        with connection.cursor() as cursor:
            # Execute the query to list all tables in the current database
            cursor.execute("SHOW TABLES;")
            tables = cursor.fetchall()  # Fetch all rows
            res = []
            for item in tables:
                res.append(item[0])
            return res
    except pymysql.MySQLError as e:
        print("Error while retrieving tables:", e)