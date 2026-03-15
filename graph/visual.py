import os
import networkx as nx
from pyvis.network import Network


def create_g_visual(G, dest_path, ds=True):
    # ds = create from datastore
    print("G", G)
    # filter graph fr frontend
    try:
        new_G = nx.Graph()
        for nid, attrs in G.nodes(data=True):
            ntype = attrs.get("type")
            graph_item = attrs.get("graph_item")
            if graph_item == "node":
                new_G.add_node(
                    nid,
                    **dict(
                        type=ntype
                    )
                )

        # 2. iter for edges between added nodes
        for nid, attrs in G.nodes(data=True):
            graph_item = attrs.get("graph_item")
            #print("graph_item", graph_item)
            if graph_item == "edge":
                #print("edge")
                trgt=attrs.get("trgt")
                src=attrs.get("src")
                eid=attrs.get("id")
                #print("trgt")
                new_G.add_edge(
                    src,
                    trgt,
                    id=eid,
                    type="edge"
                )

        options = '''
            const options = {
              "nodes": {
                "borderWidthSelected": 21,
                "font": {
                  "size": 20,
                  "face": "verdana"
                }
              }
            }
            '''

        net = Network(
            notebook=False,
            cdn_resources='in_line',
            height='1000px',
            width='100%',
            bgcolor="#222222",
            font_color="white"
        )

        net.barnes_hut()
        net.toggle_physics(True)
        net.set_options(options)

        net.from_nx(new_G)

        # Force HTML generation
        net.html = net.generate_html()
        if dest_path is not None:
            # Ensure parent dir exists (fix: Err create_g_visual [Errno 2] No such file or directory)
            parent = os.path.dirname(dest_path)
            if parent:
                os.makedirs(parent, exist_ok=True)
            with open(dest_path, 'w', encoding="utf-8") as f:
                f.write(net.html)
            print("html created and saved under:", dest_path)
        else:
            return net.html
    except Exception as e:
        print("Err create_g_visual", e)