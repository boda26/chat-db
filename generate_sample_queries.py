import random
import re


def generate_description(query):
    description = "The query "

    # Remove the LIMIT clause for easier parsing
    query_no_limit = query.split(" LIMIT ")[0]

    # Extract SELECT clause
    select_match = re.search(r"SELECT (.+?) FROM", query_no_limit)
    if select_match:
        select_clause = select_match.group(1).strip()
        if " as aggregated_value" in select_clause:
            # Aggregation present
            agg_match = re.search(r"(\w+), (\w+)\((\w+)\) as aggregated_value", select_clause)
            if agg_match:
                group_by_column = agg_match.group(1)
                agg_func = agg_match.group(2)
                numeric_column = agg_match.group(3)
                description += f"selects the {agg_func.lower()} of `{numeric_column}` grouped by `{group_by_column}`"
        else:
            # No aggregation
            columns = select_clause.split(", ")
            columns = [f"`{col}`" for col in columns]
            description += "selects the columns " + ", ".join(columns)

    # Extract FROM clause
    from_match = re.search(r"FROM (\w+)", query_no_limit)
    if from_match:
        table_name = from_match.group(1)
        description += f" from the table `{table_name}`"

    # Extract WHERE clause
    where_match = re.search(r"WHERE (.+?)(GROUP BY|ORDER BY|$)", query_no_limit)
    if where_match:
        where_condition = where_match.group(1).strip()
        description += f" where {where_condition}"

    # Extract GROUP BY clause
    group_by_match = re.search(r"GROUP BY (\w+)", query_no_limit)
    if group_by_match:
        group_by_column = group_by_match.group(1)
        description += f", grouping the results by `{group_by_column}`"

    # Extract ORDER BY clause
    order_by_match = re.search(r"ORDER BY (\w+) DESC", query_no_limit)
    if order_by_match:
        order_by_column = order_by_match.group(1)
        description += f", and orders the results by `{order_by_column}` in descending order"

    # Extract LIMIT clause
    limit_match = re.search(r"LIMIT (\d+);", query)
    if limit_match:
        limit = limit_match.group(1)
        description += f". It limits the results to {limit} rows"

    description += "."
    return description

def generate_sample_queries(connection, selected_table):
    try:
        # Fetch column names for the selected table
        with connection.cursor() as cursor:
            cursor.execute(f"DESCRIBE {selected_table}")
            columns_info = cursor.fetchall()
            columns = [row[0] for row in columns_info]
            column_data_types = {row[0]: row[1] for row in columns_info}

        if not columns:
            print(f"No columns found in the table {selected_table}.")
            return

        # Generate a random sample query
        query_type = random.choice(["simple_select", "where", "group_by", "order_by", "aggregation", "complex"])
        query = ""
        descriptive_sentence = ""

        if query_type == "simple_select":
            joined_columns = ", ".join(columns)
            columns.append(joined_columns)
            selected_columns = random.choice(columns)
            query = f"SELECT {selected_columns} FROM {selected_table} WHERE {columns[0]} IS NOT NULL LIMIT 10;"
            descriptive_sentence = f"select the first 10 rows of {selected_columns} from the table '{selected_table}'."

        elif query_type == "where":
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
                descriptive_sentence = f"selects the first 10 rows from the table '{selected_table}' that meet the following specific conditions: {', '.join(selected_conditions)}."
                print(f"\nGenerated WHERE conditions:")
                for condition in selected_conditions:
                    print(f" - {condition}")
            else:
                print("No valid conditions could be generated for the WHERE clause.")
                return

        elif query_type == "group_by":
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

        elif query_type == "order_by":
            order_by_column = random.choice(columns)
            query = f"SELECT * FROM {selected_table} ORDER BY {order_by_column} DESC LIMIT 5;"
            descriptive_sentence = f"select the first 5 rows from the table '{selected_table}', ordered by the column '{order_by_column}' in descending order."

        elif query_type == "aggregation":
            numeric_columns = [col for col in columns if col != "id"]  # Assuming 'id' is not numeric
            aggregation_function = random.choice(["MIN", "MAX", "COUNT", "AVG", "SUM"])
            agg_column = random.choice(numeric_columns)

            if aggregation_function == "MIN":
                query = f"SELECT MIN({agg_column}) as min_value FROM {selected_table};"
                descriptive_sentence = f"find the minimum value of the column '{agg_column}' in the table '{selected_table}'."
            elif aggregation_function == "MAX":
                query = f"SELECT MAX({agg_column}) as max_value FROM {selected_table};"
                descriptive_sentence = f"find the maximum value of the column '{agg_column}' in the table '{selected_table}'."
            elif aggregation_function == "COUNT":
                query = f"SELECT COUNT({agg_column}) as total_count FROM {selected_table};"
                descriptive_sentence = f"find the total number of rows in the table '{selected_table}' where the column '{agg_column}' is not NULL."
            elif aggregation_function == "AVG":
                query = f"SELECT AVG({agg_column}) as average_value FROM {selected_table};"
                descriptive_sentence = f"find the average value of the column '{agg_column}' in the table '{selected_table}'."
            elif aggregation_function == "SUM":
                query = f"SELECT SUM({agg_column}) as total_sum FROM {selected_table};"
                descriptive_sentence = f"find the sum of all values in the column '{agg_column}' from the table '{selected_table}'."

        elif query_type == "complex":
            # Generate a complex query dynamically using combinations of SQL keywords
            where_column = random.choice(columns)
            group_by_column = random.choice(columns)
            order_by_column = random.choice(columns)
            numeric_columns = [col for col in columns if
                               "int" in col or "float" in col or "double" in col or "decimal" in col]
            numeric_column = random.choice(numeric_columns) if numeric_columns else None

            query_parts = []

            # Add SELECT clause with optional aggregation
            if numeric_column and random.choice([True, False]):
                agg_func = random.choice(["SUM", "AVG", "COUNT", "MAX", "MIN"])
                query_parts.append(f"SELECT {group_by_column}, {agg_func}({numeric_column}) as aggregated_value")
                group_by_required = True
            else:
                query_parts.append(f"SELECT {group_by_column}")
                group_by_required = False

            # Add FROM clause
            query_parts.append(f"FROM {selected_table}")

            # Add WHERE clause
            if random.choice([True, False]):
                query_parts.append(f"WHERE {where_column} IS NOT NULL")

            # Add GROUP BY clause
            if group_by_required or random.choice([True, False]):
                query_parts.append(f"GROUP BY {group_by_column}")

            # Add ORDER BY clause
            if numeric_column and random.choice([True, False]):
                query_parts.append(f"ORDER BY {numeric_column} DESC")

            # Combine query parts
            query = " ".join(query_parts) + " LIMIT 10;"
            descriptive_sentence = generate_description(query)

        print(f"Generated Sample Query with query type {query_type}:\n")
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

    except ValueError:
        print("Invalid input. Please enter a valid number.")
    except Exception as e:
        print(f"Error while exploring tables: {e}")
