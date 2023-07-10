from rdflib import Literal
import pandas as pd
from tqdm import tqdm

class RDF:
    def __init__(self, graph_name: str) -> None:
        self.rdf_text = f"@base <http://{graph_name}/> . \n\n"

    def hypenate_spaces(self, val: any) -> str | None:
        if type(val) == str:
            return val.replace(" ", "-").replace("_", "-")
        elif val is not None:
            return str(val)
        else:
            return None

    def get_turtle_statement(
        self, subject: str, predicate_to_objects: dict[str, str]
    ) -> str:
        statement = f"{subject} "

        def add_object_list(objects):
            statement = ""

            for i, object in enumerate(objects):
                statement += f"{object}"

                if i != len(objects) - 1:
                    statement += ", "

            return statement

        for i, predicate_objects in enumerate(predicate_to_objects.items()):
            predicate, objects = predicate_objects

            if i != 0:
                statement += "    "

            statement += f"<{predicate}> {add_object_list(objects)} "

            if i != len(predicate_to_objects) - 1:
                statement += f";\n"

        statement += ".\n"

        return statement

    def add_node(
        self, df: pd.DataFrame, id_field_name: str, subject_type: str, progress_bar: tqdm
    ) -> None:
        rdf = ""

        for _, row in df.iterrows():
            subject = f"<{self.hypenate_spaces(subject_type)}/{self.hypenate_spaces(row[id_field_name])}/>"
            predicate_to_objects = {}

            for column in df.columns:
                if column == id_field_name:
                    continue

                subject_val = row[column]

                if type(subject_val) == str:
                    subject_val = Literal(subject_val).n3()
                    predicate_to_objects[column] = [subject_val]
                else:
                    predicate_to_objects[column] = [f'"{subject_val}"']

            rdf += self.get_turtle_statement(subject, predicate_to_objects) + "\n"
            progress_bar.update(1)

        self.rdf_text += rdf + "\n"

    def add_node_connections(self, df: pd.DataFrame, progress_bar: tqdm) -> None:
        rdf = ""

        hasCollectionField = len(df.columns) == 3

        subject_type = df.columns[0].replace("_id", "")

        for _, row in df.iterrows():
            subject_uri = f"<{self.hypenate_spaces(subject_type)}/{self.hypenate_spaces(row[df.columns[0]])}/>"
            predicate_to_objects = {}

            if hasCollectionField:
                predicate_type = self.hypenate_spaces(row["collection"])
                predicate_to_objects[predicate_type] = [
                    f"<{predicate_type}/{self.hypenate_spaces(row['id'])}/>"
                ]
            else:
                predicate_type = self.hypenate_spaces(df.columns[1].replace("_id", "").lower())
                predicate_to_objects[predicate_type] = [
                    f"<{predicate_type}/{self.hypenate_spaces(row[df.columns[1]])}/>"
                ]

            rdf += self.get_turtle_statement(subject_uri, predicate_to_objects) + "\n"
            progress_bar.update(1)

        self.rdf_text += rdf + "\n"

    def write_rdf_file(self, dir_path: str, file_name: str = "rdf") -> None:
        with open(f"{dir_path}/{file_name}.ttl", "w") as text_file:
            text_file.write(self.rdf_text)
