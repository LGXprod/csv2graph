from typing import Literal

pandas_type_literal = Literal[
    "int64" "float64" "bool" "datetime64" "timedelta[ns]" "category", "object"
]
mysql_type_literal = Literal[
    "INTEGER", "FLOAT", "BOOLEAN", "DATETIME", "DATETIME", "LONGTEXT", "unknown"
]
constraint_literal = Literal["PK", "FK"]


class Field:
    def __init__(
        self,
        name: str,
        type: pandas_type_literal,
        constraint: constraint_literal | None = None,
    ):
        self.name = name
        self.constraint = constraint

        type_mapping = {
            "int64": "INTEGER",
            "float64": "FLOAT",
            "bool": "BOOLEAN",
            "datetime64": "DATETIME",
            "timedelta[ns]": "DATETIME",
            "category": "LONGTEXT",
            "object": "unknown",
        }
        self.type = type_mapping[type]

    def is_primary_key(self):
        return self.constraint == "PK"

    def is_foreign_key(self):
        return self.constraint == "FK"

    def __str__(self):
        type = self.type

        if type == "unknown":
            if self.is_primary_key() or self.is_foreign_key():
                type = "VARCHAR"
            else:
                type = "LONGTEXT"

        field_declaration = f"{self.name} {type}"

        if self.is_primary_key():
            field_declaration += " NOT NULL"

        return field_declaration


class Table:
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.fields: list[Field] = []

    def add_field(
        self,
        name: str,
        type: pandas_type_literal,
        constraint: constraint_literal | None = None,
    ):
        self.fields.append(Field(name, type, constraint))

    def __str__(self):
        create_table_query = f"CREATE TABLE {self.table_name} (\n"
        constraints = []

        for i, field in enumerate(self.fields):
            create_table_query += f"    {str(field)}"

            if i != len(self.fields) - 1:
                create_table_query += ","
                create_table_query += "\n"

            if field.is_primary_key():
                constraints.append(f"    PRIMARY KEY ({field.name})")

            if field.is_foreign_key():
                foreign_table = field.name.split("_")[0] + "s"
                constraints.append(
                    f"    FOREIGN KEY ({field.name}) REFERENCES {foreign_table}(id)"
                )

        if len(constraints) > 0:
            create_table_query += ","
            create_table_query += "\n"

            for i, constraint in enumerate(constraints):
                create_table_query += constraint

                if i != len(self.fields) - 1:
                    create_table_query += ","

                create_table_query += "\n"
        else:
            create_table_query += "\n"

        create_table_query += ");"

        return create_table_query
