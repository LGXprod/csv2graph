import os
import pandas as pd
from tqdm import tqdm

class RedisGraph:
    def __init__(
        self,
        graph_name: str,
        node_dfs: dict[str, pd.DataFrame],
        relation_dfs: dict[str, pd.DataFrame],
        csvs_dir_path: str,
        is_undirected_graph: bool = False,
    ):
        self.script_text = f"redisgraph-bulk-insert {graph_name} --enforce-schema --skip-invalid-nodes --skip-invalid-edges"
        self.node_dfs = node_dfs
        self.relation_dfs = relation_dfs
        self.csvs_dir_path = csvs_dir_path
        self.is_undirected_graph = is_undirected_graph
        self.in_out_relation_dfs = None

    def update_in_out_relation_dfs(
        self,
        start_entity: str,
        end_entity: str,
        df: pd.DataFrame,
        out_edges_dfs: dict[str, pd.DataFrame],
    ):
        df1 = df.copy()
        df2 = df.copy()

        df1.rename(
            {
                df1.columns[0]: f":START_ID({start_entity})",
                df1.columns[1]: f":END_ID({end_entity})",
            },
            inplace=True,
            axis=1,
        )

        df2.rename(
            {
                df2.columns[1]: f":START_ID({end_entity})",
                df2.columns[0]: f":END_ID({start_entity})",
            },
            inplace=True,
            axis=1,
        )

        out_edges_dfs[f"to_{start_entity}"] = df1
        out_edges_dfs[f"to_{end_entity}"] = df2

    def add_redis_types_to_fields(self):
        for df_name, df in tqdm(self.node_dfs.items(), desc="Node Dataframes"):
            # rename fields of node dfs

            old_to_updated_field_name = {}

            for field_name, field_type in dict(df.dtypes).items():
                if "id" in field_name.lower():
                    old_to_updated_field_name[field_name] = f":ID({df_name.lower()})"
                    continue

                match str(field_type):
                    case "int64":
                        old_to_updated_field_name[field_name] = f"{field_name}:integer"
                    case "bool":
                        old_to_updated_field_name[field_name] = f"{field_name}:bool"
                    case "datetime64":
                        old_to_updated_field_name[field_name] = f"{field_name}:string"
                    case "timedelta[ns]":
                        old_to_updated_field_name[field_name] = f"{field_name}:string"
                    case "category":
                        old_to_updated_field_name[field_name] = f"{field_name}:string"
                    case "float64":
                        old_to_updated_field_name[field_name] = f"{field_name}:double"
                    case "object":
                        # basic data cleaning for fields of type object
                        first_field_val = str(df[field_name].iloc[0])

                        if first_field_val.isdigit():
                            old_to_updated_field_name[
                                field_name
                            ] = f"{field_name}:integer"
                        elif first_field_val.replace(".", "").isdigit():
                            old_to_updated_field_name[
                                field_name
                            ] = f"{field_name}:double"
                        else:
                            old_to_updated_field_name[
                                field_name
                            ] = f"{field_name}:string"

            df.rename(columns=old_to_updated_field_name, inplace=True)
            self.node_dfs[df_name] = df

        # rename fields of edge dfs

        in_out_relation_dfs = {}

        for df_name, df in tqdm(self.relation_dfs.items(), desc="Relation Dataframes"):
            field_names = list(df.columns)
            start_entity = field_names[0].replace("_id", "").lower()

            if len(df.columns) == 2:
                end_entity = field_names[1].replace("_id", "").lower()
                self.update_in_out_relation_dfs(
                    start_entity, end_entity, df, in_out_relation_dfs
                )

            if len(df.columns) == 3:
                out_entities = list(set(df["collection"].values.tolist()))

                for entity_name in out_entities:
                    collection_df = df.loc[df["collection"] == entity_name][
                        [df.columns[0], df.columns[-1]]
                    ].copy()

                    self.update_in_out_relation_dfs(
                        start_entity, entity_name, collection_df, in_out_relation_dfs
                    )

        # print("here", in_out_relation_dfs)

        self.in_out_relation_dfs = in_out_relation_dfs

    def write_bulk_csvs(self):
        # append the command string used to run the import of the csvs into the redis bulk loader

        for df_name in self.node_dfs:
            self.script_text += (
                f" -n {self.csvs_dir_path}/redis_bulk_csvs/nodes/{df_name}.csv"
            )

        for df_name in self.relation_dfs:
            self.script_text += (
                f" -r {self.csvs_dir_path}/redis_bulk_csvs/edges/{df_name}.csv"
            )

        # creating the directories for the csvs and the redis script/command to be saved

        os.makedirs(f"{self.csvs_dir_path}/redis_bulk_csvs/nodes", exist_ok=True)
        os.makedirs(f"{self.csvs_dir_path}/redis_bulk_csvs/edges", exist_ok=True)

        with open(f"{self.csvs_dir_path}/redis_bulk_csvs/redis-command.txt", "w") as f:
            f.write(self.script_text)

        # add redis field data types to field names

        self.add_redis_types_to_fields()

        # save csvs to appropriate directory

        for df_name, df in self.node_dfs.items():
            df.to_csv(
                f"{self.csvs_dir_path}/redis_bulk_csvs/nodes/{df_name}.csv", index=False
            )

        for df_name, df in self.in_out_relation_dfs.items():
            df.to_csv(
                f"{self.csvs_dir_path}/redis_bulk_csvs/edges/{df_name}.csv", index=False
            )

        print(
            "The CSVs and command line to import data using the RedisGraph Bulk Loader is saved in redis_bulk_csvs in your dataset directory."
        )
