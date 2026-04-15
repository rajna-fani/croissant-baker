"""RAI metadata schema — dataclass models for Responsible AI attributes."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


# ── AI Safety and Fairness ────────────────────────────────────────────────────


@dataclass
class AIFairnessConfig:
    data_limitations: Optional[str] = None
    data_biases: Optional[str] = None
    personal_sensitive_information: Optional[str] = None
    data_use_cases: Optional[str] = None
    data_social_impact: Optional[str] = None
    has_synthetic_data: Optional[bool] = None


# ── Data Lifecycle / Lineage ──────────────────────────────────────────────────


@dataclass
class SourceDataset:
    url: str
    id: Optional[str] = None
    name: Optional[str] = None
    organisation: Optional[str] = None
    license: Optional[str] = None


@dataclass
class ModelRef:
    url: str
    id: Optional[str] = None
    name: Optional[str] = None


@dataclass
class LineageConfig:
    source_datasets: list[SourceDataset] = field(default_factory=list)
    models: list[ModelRef] = field(default_factory=list)


# ── Activities ────────────────────────────────────────────────────────────────


@dataclass
class Agent:
    name: str
    url: Optional[str] = None
    description: Optional[str] = None
    is_synthetic: bool = False


@dataclass
class Platform:
    name: str
    url: Optional[str] = None
    description: Optional[str] = None


@dataclass
class Activity:
    id: str
    type: str  # data_collection | data_annotation | data_preprocessing
    description: Optional[str] = None
    start_at: Optional[str] = None
    end_at: Optional[str] = None
    collection_types: list[str] = field(default_factory=list)
    agents: list[Agent] = field(default_factory=list)
    platforms: list[Platform] = field(default_factory=list)


# ── Top-level config ──────────────────────────────────────────────────────────


@dataclass
class RAIConfig:
    ai_fairness: AIFairnessConfig = field(default_factory=AIFairnessConfig)
    lineage: LineageConfig = field(default_factory=LineageConfig)
    activities: list[Activity] = field(default_factory=list)
