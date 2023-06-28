"""
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

Author: Chen Haifeng
"""


import time
import argparse
import os
import yaml

import torch
import pandas as pd

from cloudtik.runtime.ai.modeling.graph_modeling.graph_sage.modeling.tokenizer import tokenize_node_ids


def _apply_embeddings_to_column(df, column, node_emb_dict, i, j):
    emb = pd.DataFrame(df[column].map(node_emb_dict).tolist()).add_prefix(
        "n{}_c{}_e".format(i, j)
    )
    df = df.join([emb])
    df.drop(
        columns=[column],
        axis=1,
        inplace=True,
    )
    return df


def _apply_embeddings(df, node_embeddings, col_map):
    if isinstance(node_embeddings, dict):
        print("Apply heterogeneous embeddings to data.")
        return _apply_heterogeneous_embeddings(
            df, node_embeddings, col_map)
    else:
        print("Apply homogeneous embeddings to data.")
        return _apply_homogeneous_embeddings(
            df, node_embeddings, col_map)


def _apply_homogeneous_embeddings(df, node_embeddings, col_map):
    node_emb_arr = node_embeddings.cpu().detach().numpy()
    node_emb_dict = {idx: val for idx, val in enumerate(node_emb_arr)}

    for i, node in enumerate(col_map.keys()):
        col_map_of_node = col_map[node]
        for j, column in enumerate(col_map_of_node.keys()):
            column_idx = col_map_of_node[column]
            df = _apply_embeddings_to_column(
                df, column_idx, node_emb_dict, i, j)

    print("CSV output shape:", df.shape)
    return df


def _apply_heterogeneous_embeddings(df, node_embeddings, col_map):
    for i, node in enumerate(col_map.keys()):
        node_emb = node_embeddings.get(node)
        if node_emb is None:
            continue

        node_emb_arr = node_emb.cpu().detach().numpy()
        node_emb_dict = {idx: val for idx, val in enumerate(node_emb_arr)}
        col_map_of_node = col_map[node]
        for j, column in enumerate(col_map_of_node.keys()):
            column_idx = col_map_of_node[column]
            df = _apply_embeddings_to_column(
                df, column_idx, node_emb_dict, i, j)

    print("CSV output shape:", df.shape)
    return df


def _map_node_embeddings(
        node_embeddings_file,
        partition_dir,
        output_file):
    nmap_file = os.path.join(partition_dir, "nmap.pt")

    if not os.path.isfile(node_embeddings_file):
        raise argparse.ArgumentTypeError(
            '"{}" is not an existing file'.format(node_embeddings_file)
        )
    print("Loading node embeddings from file")
    node_emb = torch.load(node_embeddings_file)

    # Map from partition to global
    print("Mapping from partition ids to full graph ids")
    nmap = torch.load(nmap_file)

    # nmap stores the mappings between the remapped node/edge IDs and their original ones
    # the ith element stores the original id of shuffle id i.
    if isinstance(nmap, dict):
        # handling mapping for heterogeneous graph
        orig_node_emb = {}
        for k, v in node_emb.items():
            emb = torch.zeros(v.shape, dtype=v.dtype)
            emb[nmap[k]] = v
            orig_node_emb[k] = emb
    else:
        orig_node_emb = torch.zeros(node_emb.shape, dtype=node_emb.dtype)
        orig_node_emb[nmap] = node_emb

    torch.save(orig_node_emb, output_file)


def apply_embeddings(
        processed_data_path,
        node_embeddings_file,
        output_file,
        tabular2graph,
        heterogeneous):

    with open(tabular2graph, "r") as file:
        config = yaml.safe_load(file)

    # 1. Load processed CSV file
    print("Loading processed data")
    start = time.time()
    df = pd.read_csv(processed_data_path)
    t_load_data = time.time()
    print("Time to load processed data:", t_load_data - start)

    start = time.time()

    # 2. Renumbering - generating node/edge ids starting from zero
    # homogeneous mapping because that is how the embeddings will be returned by GNN
    mapping, col_map = tokenize_node_ids(
        df, config, heterogeneous=heterogeneous)
    t_tokenize = time.time()
    print("Time to tokenize:", t_tokenize - t_load_data)

    # 3. Load node embeddings from file, add them to edge features
    # and save file for Classic ML workflow (since model is trained as homo, no mapping needed.)
    print("Loading embeddings from file")
    if not os.path.isfile(node_embeddings_file):
        raise argparse.ArgumentTypeError(
            '"{}" is not an existing file'.format(node_embeddings_file)
        )
    node_emb = torch.load(node_embeddings_file)
    df = _apply_embeddings(df, node_emb, col_map)

    # write output combining the original columns with the new node embeddings as columns
    df.to_csv(output_file, index=False)
    print("Data with embeddings save to:", output_file)
    print("Time to apply node embeddings:", time.time() - start)