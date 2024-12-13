from get_table_name import get_table_name


def explore_tables(connection):
    try:
        print("Exploring tables...")
        # Fetch table names
        tables = get_table_name(connection)

        if not tables:
            print("No tables found in the database.")
            return

        # Display table names to the user
        print("\nSelect the table you want to explore:\n")
        for i, table_name in enumerate(tables):
            print(f"{i}: {table_name}")

        # Get the user's choice
        choice = int(input("\nEnter a number: "))
        if choice < 0 or choice >= len(tables):
            print("Invalid choice. Please try again.")
            return

        selected_table = tables[choice]
        print(f"\nYou selected: {selected_table}\n")

        # Display the table schema
        print("Table Schema:")
        with connection.cursor() as cursor:
            cursor.execute(f"DESCRIBE {selected_table};")
            schema = cursor.fetchall()
            for column in schema:
                print(f"- {column[0]} ({column[1]})")

        # Display some sample data
        print("\nSample Data:")
        with connection.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {selected_table} LIMIT 5;")
            rows = cursor.fetchall()
            if rows:
                for row in rows:
                    print(row)
            else:
                print("No data found in the table.")

    except ValueError:
        print("Invalid input. Please enter a valid number.")
    except Exception as e:
        print(f"Error while exploring tables: {e}")
