import random


def generate_sample_queries_with_constructs(connection, selected_table):
    query_keyword = input("\nEnter a SQL query keyword: ")
    query_keyword = query_keyword.replace(" ", "").lower()

    # Fetch column names and data types for the selected table
    with connection.cursor() as cursor:
        cursor.execute(f"DESCRIBE {selected_table}")
        columns_info = cursor.fetchall()
        columns = [row[0] for row in columns_info]
        column_data_types = {row[0]: row[1] for row in columns_info}

    if not columns:
        print(f"No columns found in the table {selected_table}.")
        return

    numeric_columns = [col for col, dtype in column_data_types.items() if "int" in dtype or "float" in dtype or "double" in dtype or "decimal" in dtype]
    agg_column = random.choice(numeric_columns) if numeric_columns else None
    descriptive_sentence = ""

    if query_keyword == "select" or query_keyword == "from":
        # selected_columns = ", ".join(columns)
        # query = f"SELECT {selected_columns} FROM {selected_table} LIMIT 10;"
        # descriptive_sentence = f"select the first 10 rows of all columns from the table '{selected_table}'."

        joined_columns = ", ".join(columns)
        columns.append(joined_columns)
        selected_columns = random.choice(columns)
        query = f"SELECT {selected_columns} FROM {selected_table} WHERE {columns[0]} IS NOT NULL LIMIT 10;"
        descriptive_sentence = f"select the first 10 rows of {selected_columns} from the table '{selected_table}'."

    elif query_keyword == "where":
        selected_columns = ", ".join(columns)

        # Generate dynamic WHERE conditions
        where_conditions = []
        for column, dtype in column_data_types.items():
            if "int" in dtype or "float" in dtype or "double" in dtype or "decimal" in dtype:
                # Fetch min and max values for the numeric column
                with connection.cursor() as cursor:
                    cursor.execute(f"SELECT MIN({column}), MAX({column}) FROM {selected_table}")
                    min_val, max_val = cursor.fetchone()
                    if min_val is not None and max_val is not None and min_val < max_val:
                        random_value = random.randint(int(min_val), int(max_val))
                        where_conditions.append(f"{column} > {random_value}")
                        where_conditions.append(f"{column} < {random_value + 10}")  # Example range condition

            elif "char" in dtype or "text" in dtype or "varchar" in dtype:
                # Fetch a sample substring from the column
                with connection.cursor() as cursor:
                    cursor.execute(f"SELECT {column} FROM {selected_table} WHERE {column} IS NOT NULL LIMIT 1")
                    result = cursor.fetchone()
                    if result and result[0]:
                        substring = result[0][:3]  # Get a substring (e.g., first 3 characters)
                        if substring:  # Ensure the substring is not empty
                            where_conditions.append(f"{column} LIKE '%{substring}%'")

            else:
                # General condition for any column
                where_conditions.append(f"{column} IS NOT NULL")

        # Ensure a mix of condition types
        if where_conditions:
            selected_conditions = random.sample(where_conditions, k=min(3, len(where_conditions)))
            where_clause = " AND ".join(selected_conditions)
            # Generate the SELECT query
            query = f"SELECT {selected_columns} FROM {selected_table} WHERE {where_clause} LIMIT 10;"
            descriptive_sentence = f"This query selects the first 10 rows from the table '{selected_table}' that meet the following specific conditions: {', '.join(selected_conditions)}."
            print(f"\nGenerated WHERE conditions:")
            for condition in selected_conditions:
                print(f" - {condition}")
        else:
            print("No valid conditions could be generated for the WHERE clause.")
            return

    elif query_keyword == "groupby":
        if len(columns) > 1:
            group_by_column = random.choice(columns)
            aggregate_column = random.choice([col for col in columns if col != group_by_column])
            query = (f"SELECT {group_by_column}, COUNT({aggregate_column}) as count "
                     f"FROM {selected_table} GROUP BY {group_by_column};")
            descriptive_sentence = f"group rows by the column '{group_by_column}' and counts the occurrences of '{aggregate_column}'."
        else:
            query = (f"SELECT {columns[0]}, COUNT({columns[0]}) as count "
                     f"FROM {selected_table} GROUP BY {columns[0]};")
            descriptive_sentence = f"group rows by the column '{columns[0]}' and counts the occurrences."

    elif query_keyword == "orderby":
        order_by_column = random.choice(columns)
        query = f"SELECT * FROM {selected_table} ORDER BY {order_by_column} DESC LIMIT 5;"
        descriptive_sentence = f"select the first 5 rows from the table '{selected_table}', ordered by the column '{order_by_column}' in descending order."

    elif query_keyword == "min" and agg_column:
        query = f"SELECT MIN({agg_column}) as min_value FROM {selected_table};"
        descriptive_sentence = f"find the minimum value of the column '{agg_column}' in the table '{selected_table}'."
    elif query_keyword == "max" and agg_column:
        query = f"SELECT MAX({agg_column}) as max_value FROM {selected_table};"
        descriptive_sentence = f"find the maximum value of the column '{agg_column}' in the table '{selected_table}'."
    elif query_keyword == "count" and agg_column:
        query = f"SELECT COUNT({agg_column}) as total_count FROM {selected_table};"
        descriptive_sentence = f"find the total number of rows in the table '{selected_table}' where the column '{agg_column}' is not NULL."
    elif query_keyword == "avg" and agg_column:
        query = f"SELECT AVG({agg_column}) as average_value FROM {selected_table};"
        descriptive_sentence = f"find the average value of the column '{agg_column}' in the table '{selected_table}'."
    elif query_keyword == "sum" and agg_column:
        query = f"SELECT SUM({agg_column}) as total_sum FROM {selected_table};"
        descriptive_sentence = f"find the sum of all values in the column '{agg_column}' from the table '{selected_table}'."
    else:
        print(f"Invalid query keyword or no numeric column available: {query_keyword}")
        return

    print(f"Generated Sample Query with query keyword '{query_keyword}':\n")
    print(query)
    print("\nDescription:")
    print(descriptive_sentence)

    # Execute the query
    with connection.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()

    if results:
        print("\nQuery Results:")
        for row in results:
            print(row)
    else:
        print("\nNo results returned by the query.")