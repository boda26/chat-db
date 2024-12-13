import sys
import pymysql
from dotenv import load_dotenv
import os
from explore_tables import explore_tables
from add_data import add_data
from get_table_name import  get_table_name
from generate_sample_queries import generate_sample_queries
from generate_sample_queries_with_constructs import generate_sample_queries_with_constructs
from natural_language_to_query import natural_language_to_query

# Load environment variables from .env file
load_dotenv()

def main():
    # Establish the database connection at the start
    try:
        # Get database credentials from environment variables
        db_host = os.getenv("DB_HOST")
        db_user = os.getenv("DB_USER")
        db_password = os.getenv("DB_PASSWORD")
        db_name = os.getenv("DB_NAME")
        db_port = int(os.getenv("DB_PORT", 3306))  # Default to 3306 if not provided

        # Establish the database connection
        connection = pymysql.connect(
            host=db_host,
            user=db_user,
            password=db_password,
            database=db_name,
            port=db_port
        )
        print("Successfully connected to the database!")
    except pymysql.MySQLError as e:
        print("Error connecting to the database:", e)
        sys.exit(1)  # Exit if the connection fails

    print("\nWelcome to ChatDB!")

    while True:
        print("\nMain Menu:")
        print("1. Explore databases")
        print("2. Add new data to database")
        print("3. Select a database to query")
        print("4. Exit")

        try:
            choice = int(input("Enter your choice: "))

            if choice == 1:
                explore_tables(connection)
            elif choice == 2:
                add_data(connection)
            elif choice == 3:
                handle_table_querying(connection)
            elif choice == 4:
                exit_program(connection)
            else:
                print("Invalid choice! Please select a number between 1 and 6.")
        except ValueError:
            print("Invalid input! Please enter a number.")

        input("\nPress Enter to continue...")


def handle_table_querying(connection):
    try:
        # Fetch table names
        tables = get_table_name(connection)

        if not tables:
            print("No tables found in the database.")
            return

        # Display table names to the user
        print("\nSelect the table you want to query:")
        for i, table_name in enumerate(tables):
            print(f"{i}: {table_name}")

        # Get the user's choice
        choice = int(input("Enter a number: "))
        if choice < 0 or choice >= len(tables):
            print("Invalid choice. Please try again.")
            return

        selected_table = tables[choice]
        print(f"\nYou selected: {selected_table}")

        while True:
            print("\nTable Query Menu:")
            print("1. Generate Sample Query")
            print("2. Generate Sample Query with Specific Keyword")
            print("3. Natural Language to Query")
            print("4. Return to Main Menu")

            try:
                query_choice = int(input("Enter a number: "))
                if query_choice == 1:
                    generate_sample_queries(connection, selected_table)
                elif query_choice == 2:
                    generate_sample_queries_with_constructs(connection, selected_table)
                elif query_choice == 3:
                    sentence = input("\nEnter a query sentence in natural language: ")
                    natural_language_to_query(connection, sentence, selected_table)
                elif query_choice == 4:
                    return  # Exit to the main menu
                else:
                    print("Invalid choice! Please select a number between 1 and 4.")
            except ValueError:
                print("Invalid input! Please enter a number.")
            input("\nPress Enter to continue...")

    except ValueError:
        print("Invalid input. Please enter a valid number.")
    except Exception as e:
        print(f"Error while exploring tables: {e}")


def exit_program(connection):
    print("Exiting ChatDB. Goodbye!")
    # Close the database connection before exiting
    if connection:
        connection.close()
        print("Database connection closed.")
    sys.exit(0)


if __name__ == "__main__":
    main()
