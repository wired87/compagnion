"""
Thalamic Event Classifier

Unified classification facade: BrainClassifier (rule + vector) first,
AIChatClassifier (LLM) fallback when confidence is low.
"""
from .thalamic_event_classifier import ThalamicEventClassifier

__all__ = ["ThalamicEventClassifier"]
