from django.urls import path

from qbrain.graph.dj.visual import GraphLookup
from qbrain.graph.dj.brain_test import BrainTestView
from qbrain.graph.dj.thalamus_test import ThalamusTestView

app_name = "graph"
urlpatterns = [
    # client
    path('view/', GraphLookup.as_view()),
    path('brain/test/', BrainTestView.as_view()),
    path('thalamus/test/', ThalamusTestView.as_view()),
]

