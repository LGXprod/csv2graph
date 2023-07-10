import sys
from tqdm import tqdm
import pandas as pd
from .RDF import RDF


def rdf_exporter(
    graph_name: str,
    node_dfs: dict[str, pd.DataFrame],
    relation_dfs: dict[str, pd.DataFrame],
    csvs_dir_path: str,
):
    rdf_graph = RDF(graph_name)

    node_progress_bar = tqdm(
        desc="Node Counter", total=sum([len(df) for df in node_dfs.values()]), file=sys.stdout
    )

    relation_progress_bar = tqdm(
        desc="Relation Counter",
        total=sum([len(df) for df in relation_dfs.values()]),
        file=sys.stdout,
    )

    for df_name in node_dfs:
        df = node_dfs[df_name]
        rdf_graph.add_node(df, df.columns[0], df_name, node_progress_bar)

    node_progress_bar.close()

    for df in relation_dfs.values():
        rdf_graph.add_node_connections(df, relation_progress_bar)

    relation_progress_bar.close()

    rdf_graph.write_rdf_file(csvs_dir_path, graph_name)
    print(f"Written {graph_name}.ttl to {csvs_dir_path}")
