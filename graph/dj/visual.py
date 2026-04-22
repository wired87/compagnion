import json
from time import sleep

from django.http import StreamingHttpResponse
from networkx.readwrite import json_graph
from rest_framework import serializers

from rest_framework.views import APIView

from qbrain.graph.visual import create_g_visual


def event_stream(html):
    while True:
        # Simulate live variable update
        yield html
        sleep(.025)

class S(serializers.Serializer):
    g_path = serializers.CharField(
        default="g_path",
        label="g_path",
    )

class GraphLookup(APIView):
    serializer_class=S
    #GView
    def post(self, request, *args, **kwargs):

        # 1. Definiere den Pfad zur JSON-Datei
        # ACHTUNG: Hardcodierte Pfade sind schlecht. Besser wäre es, diesen Pfad aus
        # den Django-Settings oder einer Konfiguration zu laden.
        json_file_path = r"C:\Users\wired\OneDrive\Desktop\qfs\qf_sim\calculator\tree.json"

        # 2. Öffne die JSON-Datei und lade ihren Inhalt in ein Python-Dictionary
        with open(json_file_path, 'r', encoding='utf-8') as f:
            graph_data_dict = json.load(f)
        print("1")
        #pprint.pp(graph_data_dict)

        print("2")
        graph_object = json_graph.node_link_graph(graph_data_dict)
        #pprint.pp(graph_object)

        print("3")
        html = create_g_visual(graph_object, dest_path=None)
        #print("html", html)
        print("4")
        return StreamingHttpResponse(html, content_type="text/html")





    def filter_G_4_view(self, G):
        return





"""StreamingHttpResponse(
                event_stream(html_content),
                content_type='text/event-stream'
            )"""
OPTIONS = {
    "autoResize": True,
    "height": '1000px',
    "width": '100%',
    "locale": 'en',
    "locales": {},
    "clickToUse": False,
    "configure": {},
    "EDGES": {
        "color": {
            "color": "white"
        },
        "arrows": {
            "to": {
                "enabled": False,
            }
        }
    },
    "nodes": {
        "borderWidthSelected": 21,
        "font": {
            "size": 30,
            "face": "verdana",
            "color": "white"
        },
        "shape": "circle",
        "color": {
            "background": "#34495e",
            "border": "#2c3e50",
            "highlight": {
                "background": "#5dade2",
                "border": "#2e86c1"
            }
        },
        "size": 30,
        "labelHighlightBold": True,
    },
    "groups": {},
    "layout": {},
    "interaction": {
        "hover": True,
        "tooltipDelay": 200,
        "selectable": True,
        "dragNodes": True,
        "dragView": True,
        "zoomView": True,
        "multiselect": True,
        "navigationButtons": False,
        "keyboard": {
            "enabled": False,
            "speed": {
                "x": 10,
                "y": 10,
                "zoom": 0.02
            },
            "bindToWindow": True
        }
    },
    "manipulation": {},
    "physics": {
        "barnesHut": {
            "gravitationalConstant": -4000,
            "centralGravity": 0.3,
            "springLength": 200,
            "springConstant": 0.04,
            "damping": 0.09
        },
        "forceAtlas2Based": {
            "gravitationalConstant": -4000,
            "springLength": 200,
        },
        "minVelocity": 0.75,
        "solver": "barnesHut",
        "stabilization": {
            "enabled": True,
            "iterations": 1000,
            "updateInterval": 50,
            "onlyDynamicEdges": False,
            "fit": True
        }
    }
}