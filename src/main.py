import sqlparse
from sqlparse.sql import IdentifierList, Identifier, Parenthesis
from sqlparse.tokens import Keyword, DML, Punctuation

def extract_columns(ddl):
    columns = {}
    parsed = sqlparse.parse(ddl)
    for stmt in parsed:
        if stmt.get_type() == 'CREATE':
            for token in stmt.tokens:
                if isinstance(token, Parenthesis):
                    # We are inside the parenthesis where columns are defined
                    for col_def in token.tokens:
                        if isinstance(col_def, IdentifierList):
                            for identifier in col_def.get_identifiers():
                                if isinstance(identifier, Identifier):
                                    col_name = identifier.get_real_name()
                                    col_def_str = str(identifier).strip()
                                    columns[col_name] = col_def_str
                        elif isinstance(col_def, Identifier):
                            col_name = col_def.get_real_name()
                            col_def_str = str(col_def).strip()
                            columns[col_name] = col_def_str
    return columns

def compare_columns(old_columns, new_columns):
    add_columns = {}
    remove_columns = {}
    modify_columns = {}

    for col_name, col_def in new_columns.items():
        if col_name not in old_columns:
            add_columns[col_name] = col_def
        elif old_columns[col_name] != col_def:
            modify_columns[col_name] = col_def

    for col_name, col_def in old_columns.items():
        if col_name not in new_columns:
            remove_columns[col_name] = col_def

    return add_columns, remove_columns, modify_columns

def generate_alter_statements(table_name, add_columns, remove_columns, modify_columns):
    alter_statements = []

    for col_name, col_def in add_columns.items():
        alter_statements.append(f"ALTER TABLE {table_name} ADD COLUMN {col_def};")

    for col_name, col_def in remove_columns.items():
        alter_statements.append(f"ALTER TABLE {table_name} DROP COLUMN {col_name};")

    for col_name, col_def in modify_columns.items():
        col_type = ' '.join(col_def.split()[1:])
        alter_statements.append(f"ALTER TABLE {table_name} ALTER COLUMN {col_name} TYPE {col_type};")

    return alter_statements

def main(old_ddl, new_ddl, table_name):
    old_columns = extract_columns(old_ddl)
    new_columns = extract_columns(new_ddl)

    add_columns, remove_columns, modify_columns = compare_columns(old_columns, new_columns)

    alter_statements = generate_alter_statements(table_name, add_columns, remove_columns, modify_columns)

    for stmt in alter_statements:
        print(stmt)

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
        id SERIAL PRIMARY KEY,
        name VARCHAR(100),
        age INT,
        email VARCHAR(100)
    );
    """

    table_name = "example"
    main(old_ddl, new_ddl, table_name)
