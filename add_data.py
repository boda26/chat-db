import os
import csv
import pymysql


def infer_data_type(value):
    """
    Infers the SQL data type based on the value in the CSV.
    """
    try:
        int(value)
        return "INT"
    except ValueError:
        try:
            float(value)
            return "FLOAT"
        except ValueError:
            if len(value) > 255:
                return "TEXT"
            else:
                return "VARCHAR(255)"


def add_data(connection):
    try:
        # Get the path of the CSV file from the user
        file_name = input("Enter the path to the CSV file: ")

        csv_path = os.path.join("data", file_name)

        # Check if the file exists
        if not os.path.exists(csv_path):
            print("File does not exist. Please check the path and try again.")
            return

        # Get the table name from the CSV file name
        table_name = os.path.splitext(os.path.basename(csv_path))[0]

        # Open the CSV file and read the data
        with open(csv_path, 'r') as file:
            reader = csv.reader(file)
            headers = next(reader)  # Get the first row as column names
            first_row = next(reader, None)  # Read the first row of data

            if not first_row:
                print("CSV file is empty or contains only headers.")
                return

            # Infer column types from the first row of data
            column_types = [
                infer_data_type(value) for value in first_row
            ]

            # Dynamically generate the table creation query
            columns = ', '.join([
                f'`{header}` {column_type}'
                for header, column_type in zip(headers, column_types)
            ])

            create_table_query = f"""
            CREATE TABLE IF NOT EXISTS `{table_name}` (
                id INT AUTO_INCREMENT PRIMARY KEY,
                {columns}
            );
            """

            # Create the table in the database
            with connection.cursor() as cursor:
                cursor.execute(create_table_query)
                print(f"Table '{table_name}' created successfully or already exists.")

            # Insert the data into the table
            insert_query = f"""
            INSERT INTO `{table_name}` ({', '.join([f'`{header}`' for header in headers])})
            VALUES ({', '.join(['%s' for _ in headers])});
            """

            with connection.cursor() as cursor:
                cursor.execute(insert_query, first_row)  # Insert the first row
                for row in reader:  # Insert the remaining rows
                    cursor.execute(insert_query, row)
                connection.commit()

            print(f"Data from '{csv_path}' has been successfully inserted into the table '{table_name}'.")

    except Exception as e:
        print("Error while adding data:", e)
