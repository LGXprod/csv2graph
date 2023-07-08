import os
import sys
import argparse
import pandas as pd
from tqdm import tqdm

from RDF import RDF

print("CSV2Graph")

parser = argparse.ArgumentParser()

parser.add_argument(
    "-i",
    "--input",
    dest="input",
    action="store_true"
)
parser.add_argument(
    "-p", "--dirpath", dest="dirpath", default="./", help="Directory Path"
)
parser.add_argument(
    "-g", "--graphname", dest="graphname", default="graph", help="Name of Your Graph"
)

args = parser.parse_args()

nodes_dir_path = None
edges_dir_path = None
graph_name = None

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
else:
    csvs_dir_path = os.path.expanduser(args.dirpath)
    nodes_dir_path = f"{csvs_dir_path}/nodes"
    edges_dir_path = f"{csvs_dir_path}/edges"
    graph_name = args.graphname

rdf_graph = RDF(graph_name)

node_dfs = []
relation_dfs = []

for file_name in os.listdir(nodes_dir_path):
    if file_name.endswith(".csv"):
        node_dfs.append(pd.read_csv(f"{nodes_dir_path}/{file_name}"))

for file_name in os.listdir(edges_dir_path):
    if file_name.endswith(".csv"):
        relation_dfs.append(pd.read_csv(f"{edges_dir_path}/{file_name}"))

node_progress_bar = tqdm(
    desc="Node Counter", total=sum([len(df) for df in node_dfs]), file=sys.stdout
)
relation_progress_bar = tqdm(
    desc="Relation Counter",
    total=sum([len(df) for df in relation_dfs]),
    file=sys.stdout,
)

for df in node_dfs:
    rdf_graph.add_node(
        df, df.columns[0], file_name.replace(".csv", ""), node_progress_bar
    )

node_progress_bar.close()

for df in relation_dfs:
    rdf_graph.add_node_connections(df, relation_progress_bar)

relation_progress_bar.close()

rdf_graph.write_rdf_file(csvs_dir_path, graph_name)
print(f"Written {graph_name}.ttl to {csvs_dir_path}")
