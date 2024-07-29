import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Parenthesis
from sqlparse.tokens import Keyword, DML

def extract_table_name(ddl):
    parsed = sqlparse.parse(ddl)
    for stmt in parsed:
        if stmt.get_type() == 'CREATE':
            for token in stmt.tokens:
                if isinstance(token, Identifier):
                    return token.get_real_name()
                if token.ttype == DML and token.value.upper() == 'CREATE':
                    continue
                if token.ttype == Keyword and token.value.upper() == 'TABLE':
                    continue
    return None

def extract_columns(ddl):
    columns = {}
    column_data_types = {}
    parsed = sqlparse.parse(ddl)
    for stmt in parsed:
        if stmt.get_type() == 'CREATE':
            for token in stmt.tokens:
                if isinstance(token, Parenthesis):
                    # We are inside the parenthesis where columns are defined
                    col_defs = token.tokens[1:-1]  # Exclude parentheses
                    col_def = ""
                    for col_def_token in col_defs:
                        if col_def_token.is_whitespace:
                            continue
                        col_def += col_def_token.value
                        if col_def_token.value == ',' or col_def_token == col_defs[-1]:
                            parts = col_def.strip(',').strip().split()
                            col_name = parts[0]
                            col_def = ' '.join(parts)
                            columns[col_name] = col_def
                            column_data_types[col_name] = parts[1] if len(parts) > 1 else ''
                            col_def = ""
    return columns, column_data_types

def compare_columns(old_columns, new_columns):
    add_columns = {}
    remove_columns = {}
    modify_columns = {}

    for col_name, col_def in new_columns.items():
        if col_name not in old_columns:
            add_columns[col_name] = col_def
        elif old_columns[col_name] != col_def:
            modify_columns[col_name] = col_def

    for col_name in old_columns.keys():
        if col_name not in new_columns:
            remove_columns[col_name] = old_columns[col_name]

    return add_columns, remove_columns, modify_columns

def generate_alter_statements(table_name, add_columns, remove_columns):
    alter_statements = []

    for col_name, col_def in add_columns.items():
        alter_statements.append(f"ALTER TABLE {table_name} ADD COLUMN {col_def};")

    for col_name in remove_columns.keys():
        alter_statements.append(f"ALTER TABLE {table_name} DROP COLUMN {col_name};")

    return alter_statements

def generate_reverse_alter_statements(table_name, add_columns, remove_columns, old_columns):
    reverse_statements = []

    for col_name in add_columns.keys():
        reverse_statements.append(f"ALTER TABLE {table_name} DROP COLUMN {col_name};")

    for col_name, col_def in remove_columns.items():
        reverse_statements.append(f"ALTER TABLE {table_name} ADD COLUMN {col_def};")

    return reverse_statements

def main(old_ddl, new_ddl):
    table_name = extract_table_name(old_ddl)
    if not table_name:
        raise ValueError("Could not extract table name from the old DDL.")
    
    old_columns, old_column_data_types = extract_columns(old_ddl)
    new_columns, new_column_data_types = extract_columns(new_ddl)

    add_columns, remove_columns, modify_columns = compare_columns(old_columns, new_columns)

    alter_statements = generate_alter_statements(table_name, add_columns, remove_columns)
    reverse_alter_statements = generate_reverse_alter_statements(table_name, add_columns, remove_columns, old_columns)

    print("ALTER statements to update the table:")
    for stmt in alter_statements:
        print(stmt)

    print("\nALTER statements to revert the table to the old schema:")
    for stmt in reverse_alter_statements:
        print(stmt)

    print("\nColumn Data Types:")
    print("Old DDL Column Data Types:", old_column_data_types)
    print("New DDL Column Data Types:", new_column_data_types)

if __name__ == "__main__":
    old_ddl = """
    CREATE TABLE example (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100),
        age INT
    );
    """

    new_ddl = """
    CREATE TABLE example (
        name VARCHAR(100),
        age INT,
        email VARCHAR(100)
    );
    """

    main(old_ddl, new_ddl)
