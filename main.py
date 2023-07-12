import os
import sys
import argparse
import pandas as pd
import inquirer

from RDF import rdf_exporter
from redis_graph import RedisGraph
from d2rq import d2rq_exporter

print("CSV2Graph")

# Configuring flags for arguments that can be given when csv2graph is run

parser = argparse.ArgumentParser()

parser.add_argument("-i", "--input", dest="input", action="store_true")
parser.add_argument(
    "-p", "--dirpath", dest="dirpath", default="./", help="Directory Path"
)
parser.add_argument(
    "-gn", "--graphname", dest="graphname", default="graph", help="Name of Your Graph"
)
parser.add_argument(
    "-gt",
    "--graphtype",
    dest="graphtype",
    default="rdf",
    help="Type of Graph to export to",
)

args = parser.parse_args()

# the three variables below are required to run all of the converters
# (RDF, D2RQ, Cypher, Redis Bulk Loader)

nodes_dir_path = None
edges_dir_path = None
graph_name = None
graph_type = None

# if the input flag is specified at runtime then it will use inputs instead of the given flags

if args.input:
    csvs_dir_path = os.path.expanduser(input("File Path to Directory of CSVs: "))
    nodes_dir_path = f"{csvs_dir_path}/nodes"
    edges_dir_path = f"{csvs_dir_path}/edges"

    if os.path.isdir(csvs_dir_path):
        nodes_dir_exists = os.path.isdir(nodes_dir_path)
        edges_dir_exists = os.path.isdir(edges_dir_path)

        if (not nodes_dir_path) and (not edges_dir_exists):
            print("Nodes and Edges Directories do not exist")
            exit(1)

        if not nodes_dir_exists:
            print("Nodes Directory does not exist")
            exit(1)

        if not edges_dir_exists:
            print("Edges Directory does not exist")
            exit(1)

    else:
        print("CSV Directory does not exist")
        exit(1)

    while True:
        name = input("Input Graph Name: ")

        if name is not None and name.strip() != "":
            if not " " in name:
                if name[0].isalpha():
                    graph_name = name
                    break
                else:
                    print("The first character of the name must be a letter")
            else:
                print("The name must not contain any spaces")

    graph_type = inquirer.prompt(
        [
            inquirer.List(
                "graph_type",
                message="What graph type would like to export to?",
                choices=["rdf", "d2rq (MySQL)", "cypher", "redisgraph"],
            ),
        ]
    )["graph_type"]
else:
    csvs_dir_path = os.path.expanduser(args.dirpath)
    nodes_dir_path = f"{csvs_dir_path}/nodes"
    edges_dir_path = f"{csvs_dir_path}/edges"
    graph_name = args.graphname
    graph_type = args.graphtype

# reading csvs into dataframes and storing them in the dicts below using their file_name
# which will represent their dataframe name

node_dfs = {}
relation_dfs = {}

for file_name in os.listdir(nodes_dir_path):
    if file_name.endswith(".csv"):
        node_dfs[file_name.replace(".csv", "")] = pd.read_csv(
            f"{nodes_dir_path}/{file_name}"
        )

for file_name in os.listdir(edges_dir_path):
    if file_name.endswith(".csv"):
        relation_dfs[file_name.replace(".csv", "")] = pd.read_csv(
            f"{edges_dir_path}/{file_name}"
        )

match graph_type:
    case "rdf":
        rdf_exporter(graph_name, node_dfs, relation_dfs, csvs_dir_path)
    case "d2rq":
        d2rq_exporter(graph_name, node_dfs, relation_dfs, csvs_dir_path)
    case "cypher":
        pass
    case "redisgraph":
        redisGraph = RedisGraph(
            graph_name, node_dfs, relation_dfs, csvs_dir_path, False
        )
        redisGraph.write_bulk_csvs()
    case _:
        print(
            "Invalid graph type please input one of the following: rdf, d2rq, cypher, redisgraph"
        )
        exit(1)
