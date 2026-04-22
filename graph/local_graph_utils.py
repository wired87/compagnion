import json
import os
import pprint
import time
from tempfile import TemporaryDirectory

from typing import List, Dict
import networkx as nx

from qbrain.utils.serialize_complex import check_serialize_dict
from qbrain.graph.visual import create_g_visual
from qbrain.utils.manipulator import Manipulator
from qbrain.graph.utils import Utils

class GUtils(Utils):
    """
    Handles State G local and 
    History G through DataManager
    
    ALERT:
    DB Pushs need to be ahndled externally (DBManager -> _google) 
    """

    def __init__(
            self,
            G=None,
            g_from_path=None,
            nx_only=False,
            # queue: queue.Queue or None = None,
            enable_data_store=True,
            history_types=None,
            file_store=None
    ):
        super().__init__()
        self.G = None
        self.enable_data_store = enable_data_store
        self.g_from_path = g_from_path
        self.get_nx_graph(G)
        self.nx_only = nx_only
        self.history = {}

        #todo just temporary look for demo G in QFS and BB
        demo_G_save_path = r"C:\Users\bestb\PycharmProjects\BestBrain\admin_data\demo_G.json" if os.name == "nt" else "admin_data/demo_G.json"
        if os.path.isfile(demo_G_save_path):
            self.demo_G_save_path = demo_G_save_path
        else:
            self.demo_G_save_path = r"C:\Users\bestb\PycharmProjects\BestBrain\admin_data\demo_G.json" if os.name == "nt" else "admin_data/demo_G.json"

        self.manipulator = Manipulator()

        if self.enable_data_store is True:
            self.datastore = nx.Graph()
            self.history_types = history_types  # list of nodetypes captured by dataqstore  ALL_SUBS + ["ENV"]

        self.file_store=file_store or TemporaryDirectory()

        self.metadata_fields = [
            "graph_item",
            "index",
            "entry_index",
            "time",
        ]

        # Sim timestep must be updated externally for each loop
        self.timestep = None
        self.key_map = set()
        self.id_map = set()
        self.schemas = {}
        print("GUtils initialized")

    ####################################
    # CORE                             #
    ####################################

    def get_edge(self, src, trgt):
        return self.G.edges[src, trgt]

    def get_graph(self):
        return self.G

    def get_node(self, nid):
        try:
            return self.G.nodes[nid]
        except Exception as e:
            print("Err get_node:", e)
            return None

    def print_edges(self, trgt_l, src_l):
        print("len edges", len([
            attrs
            for src, trgt, attrs in self.G.edges(data=True)
            if attrs.get("src_layer").upper() == src_l.upper()
            and attrs.get("trgt_layer").upper() == trgt_l.upper()
        ]))


    def add_node(self, attrs: dict, flatten=False):
        try:
            #print("Add node:", attrs)
            attrs = self.manipulator.clean_attr_keys(
                attrs,                flatten
            )

            if attrs.get("type") is None:
                raise Exception("NODE HAS NO ATTR type")

            attrs["type"] = attrs["type"].upper()
            nid = attrs["id"]

            if self.nx_only is False:
                self.local_batch_loader(attrs)

            # CHECK UPDATE
            if self.G.has_node(nid):
                self.G.nodes[nid].update(attrs)

            self.G.add_node(nid, **{k: v for k, v in attrs.items() if k != "id"})

            # Add history entry
            #self.h_entry(nid, {k: v for k, v in attrs.items() if k != "id"})

            # Extedn keys
            self._extend_key_map(attrs)
            self._extend_id_map(nid)
            return True
        except Exception as e:
            print("Err add_node:", e)



    def h_entry(self, id, attrs, timestep=None, graph_item="node"):
        ntype = attrs.get("type", "")
        if ntype is None:
            ntype = graph_item  # -> SET EDGE

        if self.enable_data_store is True:
            if timestep is None:
                timestep = attrs.get("time", 0)

            history_id = f"{id}_{int(time.time())}_{timestep}"

            len_type_entries = len(
                [
                    (inid, iattrs)
                    for inid, iattrs in self.datastore.nodes(data=True) if
                    iattrs.get("type", "0").upper() == attrs.get("type", "1").upper()
                ]
            )

            attrs = dict(
                type=id,
                entry_index=len_type_entries,
                graph_item=graph_item,
                base_type=ntype,
                **{k: v for k, v in attrs.items() if k not in ["id", "type"]}
            )

            #print("Add H Entry:")
            #pprint.pp(attrs)

            # Extedn keys
            self._extend_key_map(attrs)
            self._extend_id_map(id)

            self.datastore.add_node(
                history_id,
                **attrs
            )
            #print("H entry node added", self.datastore.nodes[history_id])
        else:
            raise ValueError("Invalid admin_data!!!!", id, attrs)


    def add_edge(
            self,
            src=None,
            trt=None,
            attrs: dict or None = None,
            flatten=False,
            timestep=None,
            index=None
    ):
        #print(f"Add edge: {src} -> {trt}")

        # Color
        color = None

        # Check
        if index is None:
            index = attrs.get("index", None)

        if index is not None:
            color = f"rgb({index + .5}, {index + .5}, {index + .5})"

        try:
            src_layer = self.manipulator.replace_special_chars(attrs.get("src_layer")).upper()
            trgt_layer = self.manipulator.replace_special_chars(attrs.get("trgt_layer")).upper()

            # #print("src_layer", src_layer)
            # #print("trgt_layer", trgt_layer)
            if src is None:
                src = attrs.get("src")
            if trt is None:
                trt = attrs.get("trt")

            if src and trt and src_layer and trgt_layer:
                if isinstance(src, int):
                    src = str(src)
                if isinstance(trt, int):
                    trt = str(trt)
                # #print("int conv...")

                attrs = self.manipulator.clean_attr_keys(attrs, flatten)
                # #print("attrs_new", attrs )
                rel = attrs["rel"].lower().replace(" ", "_")

                edge_id = f"{src}_{rel}_{trt}"

                attrs = {
                    **attrs,
                    "src": src,
                    "trgt": trt,
                    "eid": edge_id,
                    "tid": 0,
                    "color": color,
                }

                # Add keys
                self._extend_key_map(attrs)
                self._extend_id_map(
                    attrs["eid"]
                )

                # #print(f"ids {src} -> {trt}; Layer {src_layer} -> {trgt_layer}")
                edge_table_name = f"{src_layer}_{rel}_{trgt_layer}"
                attrs["type"] = edge_table_name

                src_node_attr = {"id": src, "type": src_layer}
                trgt_node_attr = {"id": trt, "type": trgt_layer}

                if self.nx_only is False:
                    # todo run in executor
                    # #print("Upsert Local Batch Loader")
                    self.local_batch_loader(src_node_attr)
                    self.local_batch_loader(trgt_node_attr)
                    self.local_batch_loader(attrs)

                # #print("Upsert to NX")
                self.G.add_edge(src, trt, **{k: v for k, v in attrs.items()})

                if not self.G.has_node(src):
                    self.add_node(src_node_attr)

                if not self.G.has_node(trt):
                    self.add_node(trgt_node_attr)

                # Add history entry only when datastore/history is enabled.
                if self.enable_data_store is True:
                    self.h_entry(
                        id=attrs["eid"],
                        attrs={k: v for k, v in attrs.items() if k != "id"},
                        graph_item="edge"
                    )

            else:
                raise ValueError(f"Wrong edge fromat")

        except Exception as e:
            raise ValueError(f"Skipping link src: {src} -> trgt: {trt} cause:", e, attrs)


    def _extend_key_map(self, attrs):
        for k in list(attrs.keys()):
            if k not in self.key_map:
                self.key_map.add(k)


    def _extend_id_map(self, nid):
        if nid not in self.id_map:
            self.id_map.add(nid)


    def get_edges(self, src, trgt) -> list[dict or None]:
        edges = []
        if "MultiGraph" in str(type(self.G)):
            for key, edge in self.G.get_edge_data(src, trgt).items():
                edges.append(edge)
        else:
            edges.append(self.G.edges[src, trgt])
        return edges

    """def get_edges(self, datastore=True, just_id=False):
        if datastore is False:
            if just_id is True:
                edges = [attrs.get("id") for _, _, attrs in self.G.edges(data=True)]
            else:
                edges = [{"src": src, "trgt": trgt, "attrs": attrs} for src, trgt, attrs in self.G.edges(data=True)]

        else:
            edges = [{"attrs": attrs} for eid, attrs in self.datastore.edges(data=True) if
                    attrs.get("graph_item").lower() == "edge"]
        return edges"""

    def get_edges_from_node(self, nid, datastroe=True):
        new_all_edges = []

        if datastroe is False:
            all_edges = [{"src": src, "trgt": trgt, "attrs": attrs} for src, trgt, attrs in self.G.edges(data=True)]
            for edge in all_edges:
                if edge["src"] == nid or edge["trgt"] == nid:
                    new_all_edges.append(edge)
        else:
            return [{"attrs": attrs, "eid": eid} for eid, attrs in self.datastore.edges(data=True) if
                    attrs.get("graph_item").lower() == "edge"]

        if len(new_all_edges):
            all_edges = new_all_edges
        return all_edges


    def update_node(self, attrs, disable_history=False):
        nid = attrs.get("id")
        node_attrs = self.G.nodes[nid]
        if node_attrs is None:
            print("Node couldnt be updated...")
            return

        # todo serilize @ save
        #attrs = check_serialize_dict(attrs, [k for k in attrs.keys()])

        # Add keys
        self._extend_key_map(attrs)
        self.G.nodes[nid].update(attrs)

        if self.enable_data_store is True and disable_history is False:
            # Add history entry
            self.h_entry(
                attrs["id"],
                {k: v for k, v in attrs.items() if k != "id"},
                graph_item="node"
            )

    def update_edge(self, src, trgt, attrs, rels: str or list = None, temporal=False):
        # rel = attrs.get("rel", "").lower().replace(" ", "_")
        """
        src_layer = attrs.get("src_layer").upper()
        trgt_layer = attrs.get("trgt_layer").upper()
        table_name = f"{src_layer}_{rel}_{trgt_layer}
        """

        # serialize attrs
        # todo @ save chek serilize otherwise ray actors get serialized fuck in
        # attrs = check_serialize_dict(attrs, [k for k in attrs.keys()])

        # Add keys
        self._extend_key_map(attrs)

        # Update nx
        if "Graph" in str(type(self.G)):
            for key, edge in self.G.get_edge_data(src, trgt).items():
                erel = edge.get("rel")
                if erel in rels:
                    if self.enable_data_store is True:
                        edge_id = f"{src}_{erel}_{trgt}"
                        self.h_entry(
                            edge_id,
                            {k: v for k, v in attrs.items() if k != "eid"},
                            graph_item="edge"
                        )
                    self.G.edges[src, trgt, key].update(attrs)
        else:
            if self.enable_data_store is True:
                edge_id = self.G.edges[src, trgt]["eid"]
                self.h_entry(
                    edge_id,
                    {k: v for k, v in attrs.items() if k != "eid"},
                    graph_item="edge"
                )
            self.G.edges[src, trgt].update(attrs)

        # todo handle async rt spanner || fbrtdb

    ####################################
    # HELPER
    ####################################

    def get_nx_graph(self, G):
        if self.g_from_path is not None:
            if os.path.exists(self.g_from_path):
                self.load_graph()
        if G is not None:
            self.G = G
        elif self.G is None:
            self.G = nx.Graph()  # normaler G da gluon -> gluon sonst explodieren würde
        #print("Local Graph loaded")

    def save_graph(self, dest_file, ds=False):
        print("Save Gs")
        if ds is True:
            G=self.datastore
        else:
            G=self.G
        self._link_safe(
            G,
            dest_file
        )
        print(f"G admin_data written to :{dest_file}")


    def _link_safe(self, G, dest_name):
        self.check_serilize(G)
        data = nx.node_link_data(G)

        with open(f"{dest_name}", "w") as f:
            json.dump(data, f)

    def check_serilize(self, G):
        for nid, attrs in G.nodes(data=True):
            G.nodes[nid].update(
                check_serialize_dict(
                    attrs,
                    [k for k in attrs.keys()],
                )
            )
        for src, trgt, attrs in G.edges(data=True):
            G.edges[src, trgt].update(
                check_serialize_dict(
                    attrs,
                    [k for k in attrs.keys()],
                )
            )
        return G


    def load_graph(self, local_g_path=None):
        if local_g_path is None:
            local_g_path = self.g_from_path
        """Loads the networkx graph from a JSON file."""
        print(f"📂 Loading graph from {local_g_path}...")
        with open(local_g_path, "r", encoding="utf-8") as f:
            graph_data = json.load(f)  # Use json.load() for files, not json.loads()

        self.G = nx.node_link_graph(graph_data)

        # return env
        for k, v in self.G.nodes(data=True):
            type = v.get("type")
            if type == "ENV":
                return k, v
        print(f"✅ Graph loaded! {len(self.G.nodes)} nodes, {len(self.G.edges)} edges.")

    def print_status_G(self):
        print("STATUS:", self.G)
        everything = {}
        for k, v in self.G.nodes(data=True):
            ntype = v.get("type")
            if ntype not in everything:
                everything[ntype] = []
            everything[ntype].append(k)

        for k, v in everything.items():
            print(f"{k}: {len(v)} nodes:")#
            pprint.pp(v)

    def local_batch_loader(self, args):
        table_name = args.get("type")
        row_id = args.get("id", args.get("eid"))
        if table_name:
            if table_name not in self.schemas:
                self.schemas[table_name] = {
                    "schema": {},
                    "rows": [],
                    "id_map": set(),
                }
                #print(f"Added {table_name} to schema")

            if row_id not in [item for item in self.schemas[table_name]["id_map"]]:
                # #print(f"Insert {row_id} into {table_name}")
                self.schemas[table_name]["rows"].append(args)
                self.schemas[table_name]["id_map"].add(row_id)
            # else:
            # #print(f"{row_id} already in schema")
        # #print("Added args")

    def get_single_neighbor_nx(self, node, target_type:str):
        #print("Node", node)
        try:
            if isinstance(node, tuple):
                node = node[0]
            for neighbor in self.G.neighbors(node):
                if self.G.nodes[neighbor].get('type') == target_type:
                    return neighbor, self.G.nodes[neighbor]
            return None, None  # No neighbor of that type found
        except Exception as e:
            print(f"Couldnt fetch content: {e}")

    def get_node_list(self, trgt_types, just_id=False):
        interest = {
            nid: attrs
            for nid, attrs in self.G.nodes(data=True)
            if attrs.get("type") in trgt_types
        }
        if just_id is True:
            interest = list(interest.keys())
        return interest


    def get_edge_ids(self, src, neighbor_ids):
        eids = []
        for nnid in neighbor_ids:
            eattrs = self.G.get_edge_data(src, nnid)
            if "eid" in eattrs:
                eid = eattrs["id"]
            else:
                rel = eattrs.get("rel")
                eid = f"{src}_{rel}_{nnid}"
            eids.append(eid)
        #print(f"Edge Ids extracted: {eids}")
        return eids



    def get_neighbor_list(
            self,
            node,
            target_type: str or list or None = None,
            just_ids=False
    ) -> List[str] or Dict[str, Dict]:
        neighbors = {}

        # Filter Input
        if isinstance(target_type, str):
            target_type = [target_type]
        upper_trgt_types = [t.upper() for t in target_type]

        if just_ids is True:
            nids = list(self.G.neighbors(node))
            #print(f"Node Ids extracted: {nids}")
            return nids

        for neighbor in self.G.neighbors(node):
            # Get neighbor from type
            nattrs = self.G.nodes[neighbor]
            if target_type is not None:
                ntype = nattrs.get('type').upper()
                if ntype in upper_trgt_types:
                    if neighbor not in neighbors:
                        neighbors[neighbor] = {}
                    neighbors[neighbor] = nattrs

        #print(f"Neighbors extracted: {neighbors.keys()}")
        return neighbors


    def get_neighbor_list_rel(
            self,
            node:str,
            trgt_rel: str or list or None = None,
            as_dict=False
    ):
        neighbors = {}
        edges = {}

        if isinstance(trgt_rel, str):
            trgt_rel = [trgt_rel]

        # Get neighbor from rel
        for nnid in self.G.neighbors(node):
            edge_data = self.G.get_edge_data(node, nnid)

            try:
                if isinstance(self.G, (nx.MultiGraph, nx.MultiDiGraph)):
                    for key, edge_attrs in edge_data.items():
                        ntype = edge_attrs.get('type')

                        if edge_attrs.get("rel") in trgt_rel:
                            if ntype not in neighbors:
                                neighbors[nnid] = {}
                            edges[nnid] = edge_attrs
                else:
                    # check if rel matches
                    if edge_data.get("rel").lower() in [rel.lower() for rel in trgt_rel]:
                        # get nodes from extracted edges
                        attrs = self.G.nodes[nnid]
                        neighbors[nnid] = {
                            "id": nnid,
                            **{
                                k: v
                                for k, v in attrs.copy().items()
                                if k != "id"
                            }
                        }

            except Exception as e:
                print(f"Err get_neighbor_list_rel for ({edge_data}):", e)

        if as_dict is True:
            return neighbors

        return [
            (nid, attrs)
            for nid, attrs in neighbors.items()
        ]

    def remove_node(self, node_id, ntype):
        for row in self.schemas[ntype]["rows"]:
            if row["id"] == node_id:
                self.schemas[ntype]["rows"].remove(row)
                break
        self.G.remove_node(node_id)


    def cleanup_self_schema(self):
        # #print("Cleanup schema")
        for k, v in self.schemas.items():
            v["rows"] = []


    def build_G_from_data(
            self,
            initial_data,
            env_id=None,
            save_demo=False,
    ):
        # --- Graph aufbauen ---
        env = None
        data_keys = [k for k in initial_data.keys()]
        print(f"INITIAL DATA KEYS: {data_keys}")

        for node_type, node_id_data in initial_data.items():
            # Just get valid
            nupper = node_type.upper()
            valid_types = []
            nupper_valid_t = nupper in valid_types

            print(f"{nupper} valid: {nupper_valid_t}")

            if nupper_valid_t:
                if isinstance(node_id_data, dict):  # Sicherstellen, dass es ein Dictionary ist
                    for nid, attrs in node_id_data.items():
                        # print(f">>>NID, {nid}")
                        if node_type.lower() == "EDGES":
                            parts = nid.split(f"_{attrs.get('rel')}_")
                            # print("parts", parts)
                            # check 2 ids in id and
                            if len(parts) >= 2:
                                self.add_edge(
                                    parts[0],
                                    parts[1],
                                    attrs=attrs
                                )
                            else:
                                print("something else!!!")

                        elif node_type == "ENV":
                            print("Env recognized")
                            env = attrs
                            env_id = nid
                            self.add_node(
                                attrs=attrs,
                            )
                            # Speichern Sie die env_id, falls benötigt
                        else:
                            self.add_node(
                                attrs=attrs,
                            )
                else:
                    print(f"DATA NOT A DICT:{node_type}:{node_id_data}")
                    # pprint.pp(node_id_data)
                # time.sleep(10)

            else:
                print(f"TYPE NOT VALID:{node_type}")

        print(f"Graph successfully build: {self.G}")

        if save_demo is True and getattr(self, "demo_G_save_path", None) is not None:
            self.save_graph(dest_file=self.demo_G_save_path)
        return env, env_id

    def delete_node(self, delid):
        if delid and self.G.has_node(delid):
            #print(f"Del node {delid}")
            self.G.remove_node(delid)
        else:
            print(f"Couldnt delete since {delid} doesnt exists")
    
    
    def get_node_pos(self, G=None):
        if G==None:
            G = self.G
        serializable_node_copy = []
        valid_types = []
        for nid, attrs in G.nodes(data=True):
            ntype = attrs.get("type")
            if ntype in valid_types:
                # todo single subs
                serializable_node_copy.append(
                    {
                        "id": nid,
                        "pos": attrs.get("pos")
                    }
                )
        return serializable_node_copy


    def get_nodes(
            self,
            filter_key=None,
            filter_value:str or list=None,
            just_id=False,
    ) -> list[int] or list[tuple]:
        #print("G:", self.G)
        nodes = self.G.nodes(data=True)

        #print(f"len nodes: {len(nodes)}")

        if filter_key is not None and filter_value is not None:
            new_nodes = []
            if not isinstance(filter_value, list):
                filter_value = [filter_value]

            for nid, attrs in nodes:
                if attrs.get(filter_key) in filter_value:
                    if just_id is True:
                        new_nodes.append(
                            nid
                        )
                    else:
                        new_nodes.append(
                            (nid, attrs)
                        )
            nodes = new_nodes
        print("get_nodes... done")
        return nodes

    
    def get_edges_src_trgt_pos(self, G=None, get_pos=False) -> list[dict]:
        if G == None:
            G = self.G
        edges=[]
        valid_types = []
        for src, trgt, attrs in G.edges(data=True):
            src_attrs = G.nodes[src]
            trgt_attrs = G.nodes[trgt]

            src_type = src_attrs["type"]
            trgt_type = trgt_attrs["type"]

            if src_type in valid_types and trgt_type in valid_types:
                if get_pos is True:
                    src_pos = src_attrs["pos"]
                    trgt_pos = trgt_attrs["pos"]

                    # todo calc weight based on
                    edges.append(
                        dict(
                            src=src_pos,
                            trgt=trgt_pos
                        )
                    )
                else:
                    edges.append(
                        dict(
                            src=src,
                            trgt=trgt
                        )
                    )
        #print(f"edge src trgt pos set: {edges}")
        return edges

    def create_html(self):
        save_path = os.path.join(
            self.file_store.name,
            "graph.html",
        )
        html = create_g_visual(self.datastore, dest_path=None)
        with open(save_path, "w", encoding="utf-8") as f:
            f.write(html)
        print(f"HTML Graph was written to: {save_path}")





    def categorize_nodes_in_types(self, valid_ntypes) -> dict[list]:
        categorized = {}
        for nid, attrs in self.G.nodes(data=True):
            ntype = attrs.get("type")
            if ntype:
                ntype=ntype.upper()
            if ntype in [n.upper() for n in valid_ntypes]:
                if ntype not in categorized:
                    categorized[ntype] = []
                categorized[ntype].append(
                    (nid, attrs)
                )
        print("Nodes in types categorized")
        return categorized

    def categorize_nodes_in_qfns(self) -> dict[list[tuple]]:
        categorized = {}
        points = [(nid, attrs) for nid, attrs in self.G.nodes(data=True) if attrs.get("type") == "PIXEL"]

        for qfn in points:
            qfn_id = qfn[0]
            categorized[qfn_id] = self.get_neighbor_list_rel(qfn_id, trgt_rel="has_field")

        print("Nodes in PIXELs categorized")
        return categorized


    ###################
    # GETTER
    ###################

    def get_demo_G_save_path(self):
        return self.demo_G_save_path

    def get_env(self):
        """env:tuple = [(nid, attrs) for nid, attrs in self.G.nodes(admin_data=True) if attrs.get("type") == "ENV"][0]
        return {"id": env[0], **{k:v for k,v in env[1].items() if k != "id"}}"""
        for nid, attrs in self.G.nodes(data=True):
            if attrs.get("type") == "ENV":
                print("ENV entry found")
                return {
                    "id": nid,
                    **{k: v for k, v in attrs.items() if k != "id"}}

