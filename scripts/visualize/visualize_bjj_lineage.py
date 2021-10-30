"""
This script visualizes the csv that has parent_child_relationship
"""
import sys
import json
import pandas as pd
import matplotlib.pyplot as plt
import networkx as nx
from networkx.readwrite import json_graph
import plotly.graph_objects as go
import random
import graphviz

def hierarchy_pos(G, root=None, width=2, vert_gap = 5, vert_loc = 0, xcenter = 0.5):
    '''
    From Joel's answer at https://stackoverflow.com/a/29597209/2966723.  
    Licensed under Creative Commons Attribution-Share Alike 
    If the graph is a tree this will return the positions to plot this in a 
    hierarchical layout.
    G: the graph (must be a tree)
    root: the root node of current branch 
    - if the tree is directed and this is not given, 
    the root will be found and used
    - if the tree is directed and this is given, then 
    the positions will be just for the descendants of this node.
    - if the tree is undirected and not given, 
    then a random choice will be used.                                              width: 
    horizontal space allocated for this branch 
    - avoids overlap with other branches                                            vert_gap: gap between levels of hierarchy                                       vert_loc: vertical location of root
    xcenter: horizontal location of root
    '''
    if not nx.is_tree(G):
        raise TypeError(
                'cannot use hierarchy_pos on a graph that is not a tree')
    if root is None:
        if isinstance(G, nx.DiGraph):
            root = next(iter(nx.topological_sort(G)))  
            #allows back compatibility with nx version 1.11
        else:
            root = random.choice(list(G.nodes))
    def _hierarchy_pos(G, root, width=2, vert_gap = 5, vert_loc = 0, 
        xcenter = 0.5,pos = None, parent = None):
        '''see hierarchy_pos docstring for most arguments
        pos: a dict saying where all nodes go if they have been assigne
        parent: parent of this branch. - only affects it if non-directe
        '''
        if pos is None:
            pos = {root:(xcenter,vert_loc)}
        else:
            pos[root] = (xcenter, vert_loc)
        children = list(G.neighbors(root))
        if not isinstance(G, nx.DiGraph) and parent is not None:
            children.remove(parent)  
        if len(children)!=0:
            dx = width/len(children) 
            nextx = xcenter - width/2 - dx/2
            for child in children:
                nextx += dx
                pos = _hierarchy_pos(
                        G,
                        child, 
                        width = dx, 
                        vert_gap = vert_gap,
                        vert_loc = vert_loc-vert_gap, 
                        xcenter=nextx,pos=pos, 
                        parent = root)
        return pos
    return _hierarchy_pos(G, root, width, vert_gap, vert_loc, xcenter)

def main(in_path, out_image_path):
    """
    Parameters
    ------------
    in_path: str
        the path to the parent_child_relationship containg all edges
        of graph

    out_image_path: str
        path to static image export
    """
    
    ent_parent_ids = pd.read_csv(in_path)
    G = nx.Graph()
    fig = plt.figure(figsize=(40, 40))
    #g = graphviz.Digraph()
    for ent_id, parent_id in zip(
            ent_parent_ids["entity_id"].to_list(),
            ent_parent_ids["parent_id"].to_list()
            ):
        #g.edge(str(ent_id), str(parent_id))
        G.add_edge(ent_id, parent_id)
    #pos = hierarchy_pos(G,0)
    nx.draw_kamada_kawai(G, node_size=40, linewidths=0.1)
    data = json_graph.node_link_data(G)
    with open('data.json', 'w') as f:
            json.dump(data, f)
    fig.savefig("hierarchy")
if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])

