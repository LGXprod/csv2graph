import pandas as pd
from typing import Literal
from dateutil.parser import parse


def is_date(string, fuzzy=False):
    """
    Return whether the string can be interpreted as a date.

    :param string: str, string to check for date
    :param fuzzy: bool, ignore unknown tokens in string if True
    """
    if len(string) == 10:
        try:
            parse(string, fuzzy=fuzzy)
            return True

        except ValueError:
            return False
    else:
        return False


def get_pd_object_type(
    df: pd.DataFrame, field_name: str, class_type: Literal["mysql", "redis"]
):
    # basic data cleaning for fields of type object

    match str(df[field_name].dtype):
        case "int64":
            return f"{field_name}:integer" if class_type == "redis" else "INTEGER"
        case "bool":
            return f"{field_name}:bool" if class_type == "redis" else "BOOLEAN"
        case "datetime64":
            return f"{field_name}:string" if class_type == "redis" else "DATETIME"
        case "timedelta[ns]":
            return f"{field_name}:string" if class_type == "redis" else "DATETIME"
        case "category":
            return f"{field_name}:string" if class_type == "redis" else "LONGTEXT"
        case "float64":
            return f"{field_name}:double" if class_type == "redis" else "FLOAT"
        case "object":
            is_integer = True
            is_float = True
            is_date = True

            for i, row in df.iterrows():
                val = row[field_name]

                if not is_integer and not is_float and not is_date:
                    if class_type == "redis":
                        return f"{field_name}:string"

                    if class_type == "mysql":
                        # use text size to determine mysql text type
                        max_length = df[field_name].map(len).max()

                        if max_length <= 65535:
                            return "VARCHAR"
                        elif max_length <= 16777215:
                            return "MEDIUMTEXT"
                        else:
                            return "LONGTEXT"

                if is_integer and not val.isdigit():
                    is_integer = False

                if is_float and not val.replace(".", "").isdigit():
                    is_float = False

                if is_date and not is_date(val):
                    is_date = False

            if is_integer:
                return f"{field_name}:integer" if class_type == "redis" else "INTEGER"

            if is_float:
                return f"{field_name}:double" if class_type == "redis" else "FLOAT"

            if is_date:
                return f"{field_name}:string" if class_type == "redis" else "DATETIME"
