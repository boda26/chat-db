import nltk
from nltk.tokenize import word_tokenize
from nltk.corpus import stopwords
from nltk import pos_tag, ne_chunk
import re

# nltk.download('punkt')
# nltk.download('averaged_perceptron_tagger')
# nltk.download('maxent_ne_chunker')
# nltk.download('words')
# nltk.download('stopwords')

def natural_language_to_query(connection, sentence, selected_table):
    query = translate_to_sql(sentence, connection, selected_table)
    print("\nGenerated SQL Query:")
    print(query)

    with connection.cursor() as cursor:
        cursor.execute(query)
        results = cursor.fetchall()

    if results:
        print("\nQuery Results:")
        for row in results:
            print(row)
    else:
        print("\nNo results returned by the query.")

def get_table_structure(connection, selected_table):
    """
    Retrieve the structure of a given table from the database.
    """
    with connection.cursor() as cursor:
        cursor.execute(f"DESCRIBE {selected_table}")
        columns_info = cursor.fetchall()
        column_data_types = {row[0]: row[1] for row in columns_info}
    return column_data_types

def translate_to_sql(sentence, connection, selected_table):
    """
    Translate an English sentence into a SQL query using improved parsing.
    """
    # Fetch table structure
    table_structure = get_table_structure(connection, selected_table)
    table_columns = list(table_structure.keys())

    # Define synonyms for mapping
    synonyms = {
        'revenue': 'sales_amount',
        'amount': 'sales_amount',
        'category': 'product_category',
        'name': 'title',  # Map 'name' to 'title'
    }

    # Tokenize the sentence
    tokens = word_tokenize(sentence.lower())

    # Join tokens back to a string for pattern matching
    sentence_lower = ' '.join(tokens)

    # Split the sentence at commas to separate main query and order instructions
    parts = [part.strip() for part in sentence_lower.split(',')]

    # The main query is the first part
    main_sentence = parts[0]

    # Initialize order_by components
    order_by_clause = ""

    # If there is a second part, attempt to extract order by instructions
    if len(parts) > 1:
        order_sentence = ', '.join(parts[1:])  # In case there are multiple commas
        order_by_components = extract_order_by(order_sentence, table_columns, synonyms)
        if order_by_components:
            order_by_clause = f" ORDER BY {order_by_components['column']} {order_by_components['direction']}"

    # Attempt to detect if the main sentence matches a known pattern
    pattern_name, components = detect_pattern(main_sentence)
    if pattern_name:
        # Map components to columns
        mapped_columns = map_components_to_columns(components, table_columns)
        query = generate_aggregate_query(mapped_columns, selected_table, pattern_name, order_by_clause)
        if query:
            return query  # Return the generated query
        else:
            # If mapping failed, proceed with existing code
            pass

    # Initialize query components
    select_columns = []
    conditions = []
    action = "SELECT"

    # Function to normalize text
    def normalize(text):
        return re.sub(r'\s+|_', '', text.lower())

    # Extract attributes (columns) from the main sentence
    attributes = extract_attributes(word_tokenize(main_sentence))
    if attributes:
        # Split attributes by 'and' or commas
        attribute_list = re.split(r'\s+and\s+|\s*,\s*', attributes)
        matched_columns = []
        for attr in attribute_list:
            normalized_attr = normalize(attr)
            # Check synonyms
            normalized_attr = synonyms.get(normalized_attr, normalized_attr)
            for column in table_columns:
                normalized_column = normalize(column)
                if normalized_attr == normalized_column:
                    matched_columns.append(column)
                    break
        if matched_columns:
            select_columns = matched_columns
        else:
            select_columns = ["*"]
    else:
        select_columns = ["*"]

    # Operator map with multi-word operators
    operator_map = {
        'greater than': '>',
        'is greater than': '>',
        'bigger than': '>',
        'is bigger than': '>',
        'more than': '>',
        'is more than': '>',
        'higher than': '>',
        'is higher than': '>',
        'larger than': '>',
        'is larger than': '>',
        'exceeds': '>',
        'above': '>',
        'less than': '<',
        'is less than': '<',
        'lower than': '<',
        'is lower than': '<',
        'smaller than': '<',
        'is smaller than': '<',
        'below': '<',
        'under': '<',
        'equal to': '=',
        'equals to': '=',
        'equals': '=',
        'equal': '=',
        'is': '=',
        'between': 'BETWEEN'
    }

    # Build patterns for operators, sorted by length to match longer phrases first
    operator_phrases = sorted(operator_map.keys(), key=lambda x: -len(x))
    operator_pattern = '|'.join(map(re.escape, operator_phrases))

    # Extract conditions from the main sentence
    stop_words = set(stopwords.words('english'))
    for column in table_columns:
        column_lower = column.lower().replace('_', ' ')  # Handle underscores

        # Build a regex pattern for each operator
        patterns = []

        # BETWEEN operator
        patterns.append(
            rf'{column_lower}\s+(?:is\s+)?between\s+(\d+\.?\d*)\s+and\s+(\d+\.?\d*)'
        )

        # Other operators
        for op_phrase in operator_phrases:
            if op_phrase != 'between':
                patterns.append(
                    rf'{column_lower}\s+(?:is\s+)?{re.escape(op_phrase)}\s+(\d+\.?\d*|\w+)'
                )

        # Equality without operator phrases
        patterns.append(
            rf'{column_lower}\s+(?:is\s+)?(\d+\.?\d*|\w+)'
        )

        # Try matching patterns
        for pattern in patterns:
            regex = re.compile(pattern, re.IGNORECASE)
            match = regex.search(main_sentence)
            if match:
                groups = match.groups()
                if 'between' in pattern:
                    value1, value2 = groups
                    conditions.append(f"{column} BETWEEN {value1} AND {value2}")
                else:
                    value = groups[-1]
                    # Skip if value is a stopword
                    if value.lower() in stop_words:
                        continue
                    # Extract the operator from the matched string
                    op_match = re.search(operator_pattern, match.group())
                    if op_match:
                        op_phrase = op_match.group()
                        op_symbol = operator_map.get(op_phrase.lower(), '=')
                    else:
                        op_symbol = '='
                    # Determine if value is numeric
                    if re.match(r'^\d+(\.\d+)?$', value):
                        conditions.append(f"{column} {op_symbol} {value}")
                    else:
                        conditions.append(f"{column} {op_symbol} '{value}'")
                break  # Stop after the first match

    # Build the SQL query
    query = f"{action} {', '.join(set(select_columns))} FROM {selected_table}"

    if conditions:
        query += f" WHERE {' AND '.join(conditions)}"

    if order_by_clause:
        query += order_by_clause

    query += ";"
    return query

def extract_order_by(sentence, table_columns, synonyms):
    """
    Extract ORDER BY instructions from the sentence.
    """
    order_by_components = {}

    # Patterns to detect order by clauses
    order_patterns = [
        r'order(?:ed)?\s+by\s+(\w+(?:\s\w+)*)(?:\s+in\s+)?(ascending|descending|asc|desc)\s*',
        r'order(?:ed)?\s+by\s+(ascending|descending|asc|desc)(?:\s+in\s+)?(\w+(?:\s\w+)*)\s*',
        r'order(?:ed)?\s+by\s+(\w+(?:\s\w+)*)\s*',
    ]

    for pattern in order_patterns:
        regex = re.compile(pattern, re.IGNORECASE)
        match = regex.search(sentence)
        if match:
            groups = match.groups()
            # Determine if the direction is first or second group
            if groups[0].lower() in ['ascending', 'descending', 'asc', 'desc']:
                direction = groups[0]
                column_name = groups[1] if len(groups) > 1 else None
            else:
                column_name = groups[0]
                direction = groups[1] if len(groups) > 1 else 'asc'

            # Normalize and map column name
            if column_name:
                normalized_column = column_name.lower().replace(' ', '_')
                normalized_column = synonyms.get(normalized_column, normalized_column)
                for column in table_columns:
                    if normalized_column == column.lower():
                        order_by_components['column'] = column
                        break
                else:
                    order_by_components['column'] = None  # Column not found
            else:
                order_by_components['column'] = None

            # Normalize direction
            if direction and direction.lower() in ['ascending', 'asc']:
                order_by_components['direction'] = 'ASC'
            elif direction and direction.lower() in ['descending', 'desc']:
                order_by_components['direction'] = 'DESC'
            else:
                order_by_components['direction'] = 'ASC'  # Default to ascending

            # Only return if column is found
            if order_by_components['column']:
                return order_by_components

    return None

def extract_attributes(tokens):
    """
    Extract attributes (columns) from the tokens.
    """
    verbs = ['find', 'get', 'show', 'select']
    prepositions = ['of', 'from', 'in', 'where']
    attributes = []
    start_extracting = False
    for i, token in enumerate(tokens):
        if token in verbs and not start_extracting:
            start_extracting = True
            continue
        if start_extracting:
            if token in prepositions:
                break
            attributes.append(token)
    return ' '.join(attributes)

def detect_pattern(sentence):
    """
    Detect if the sentence matches any predefined patterns.
    """
    patterns = {
        'total_A_by_B': r'(?:total|sum)\s+(?P<A>\w+(?:\s\w+)*)\s+(?:broken\s+down\s+by|by)\s+(?P<B>\w+(?:\s\w+)*)',
        'average_A_by_B': r'average\s+(?P<A>\w+(?:\s\w+)*)\s+(?:broken\s+down\s+by|by)\s+(?P<B>\w+(?:\s\w+)*)',
        'highest_A_by_B': r'(?:highest|largest|greatest)\s+(?P<A>\w+(?:\s\w+)*)\s+(?:broken\s+down\s+by|by)\s+(?P<B>\w+(?:\s\w+)*)',
        'lowest_A_by_B': r'(?:lowest|smallest|least)\s+(?P<A>\w+(?:\s\w+)*)\s+(?:broken\s+down\s+by|by)\s+(?P<B>\w+(?:\s\w+)*)',
        'count_A_by_B': r'count\s+(?P<A>\w+(?:\s\w+)*)\s+(?:broken\s+down\s+by|by)\s+(?P<B>\w+(?:\s\w+)*)',
    }

    for pattern_name, pattern_regex in patterns.items():
        match = re.search(pattern_regex, sentence)
        if match:
            return pattern_name, match.groupdict()
    return None, None

def map_components_to_columns(components, table_columns):
    """
    Map the extracted components to actual database column names.
    """
    synonyms = {
        'revenue': 'sales_amount',
        'amount': 'sales_amount',
        'category': 'product_category',
        'name': 'title',  # Map 'name' to 'title'
    }

    mapped_columns = {}
    for key, value in components.items():
        normalized_value = value.lower().replace(' ', '_')
        normalized_value = synonyms.get(normalized_value, normalized_value)

        for column in table_columns:
            if normalized_value == column.lower():
                mapped_columns[key] = column
                break
        else:
            mapped_columns[key] = None
    return mapped_columns

def generate_aggregate_query(mapped_columns, selected_table, pattern_name, order_by_clause=""):
    """
    Generate SQL query based on the mapped columns and pattern.
    """
    if None in mapped_columns.values():
        return None

    A = mapped_columns['A']
    B = mapped_columns['B']

    if pattern_name == 'total_A_by_B':
        query = f"SELECT {B}, SUM({A}) AS total_{A} FROM {selected_table} GROUP BY {B}"
    elif pattern_name == 'average_A_by_B':
        query = f"SELECT {B}, AVG({A}) AS average_{A} FROM {selected_table} GROUP BY {B}"
    elif pattern_name == 'highest_A_by_B':
        query = f"SELECT {B}, MAX({A}) AS highest_{A} FROM {selected_table} GROUP BY {B}"
    elif pattern_name == 'lowest_A_by_B':
        query = f"SELECT {B}, MIN({A}) AS lowest_{A} FROM {selected_table} GROUP BY {B}"
    elif pattern_name == 'count_A_by_B':
        query = f"SELECT {B}, COUNT({A}) AS count_{A} FROM {selected_table} GROUP BY {B}"
    else:
        return None

    if order_by_clause:
        query += order_by_clause

    query += ";"
    return query
