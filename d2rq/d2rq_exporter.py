import pandas as pd
from .d2rq import Table


def d2rq_exporter(
    graph_name: str,
    node_dfs: dict[str, pd.DataFrame],
    relation_dfs: dict[str, pd.DataFrame],
    csvs_dir_path: str,
):
    tables = []
    insert_queries = []

    for df_name, df in node_dfs.items():
        table = Table(df_name)

        for field_name, field_type in dict(df.dtypes).items():
            constraint = "PK" if field_name == "id" or "_id" in field_name else None
            table.add_field(field_name, str(field_type), constraint)

        tables.append(table)

    for df_name, df in relation_dfs.items():
        if len(df.columns) == 2:
            table = Table(df_name.lower())

            [fk_name_1, fk_name_2] = list(df.columns)
            types = dict(df.dtypes)

            table.add_field(fk_name_1, str(types[fk_name_1]), "FK")
            table.add_field(fk_name_2, str(types[fk_name_2]), "FK")

            tables.append(table)

            continue

        if len(df.columns) == 3:
            fk_name_1 = df_name.replace("_junction", "").lower()
            fk_names = list(set(df["collection"].values.tolist()))
            types = dict(df.dtypes)

            for fk_name_2 in fk_names:
                [col_1, _, col_2] = df.columns
                table = Table(df_name.lower())

                table.add_field(fk_name_1, str(types[col_1]), "FK")
                table.add_field(fk_name_2, str(types[col_2]), "FK")

                tables.append(table)

    sql_script = (
        f"CREATE DATABASE {graph_name.lower()}_db;\n\nUSE {graph_name.lower()}_db;\n\n"
    )

    sql_script += "\n\n".join([str(table) for table in tables])

    with open(f"{csvs_dir_path}/{graph_name}.sql", "w") as f:
        f.write(sql_script)
