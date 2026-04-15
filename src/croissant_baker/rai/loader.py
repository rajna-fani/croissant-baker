"""Load and validate a RAI config YAML file into a RAIConfig dataclass."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import yaml

from croissant_baker.rai.schema import (
    Activity,
    Agent,
    AIFairnessConfig,
    LineageConfig,
    ModelRef,
    Platform,
    RAIConfig,
    SourceDataset,
)


def _str(value) -> Optional[str]:
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def load_rai_config(path: Path) -> RAIConfig:
    """Load a RAI YAML config file and return a RAIConfig instance."""
    with open(path, encoding="utf-8") as fh:
        raw = yaml.safe_load(fh) or {}

    # AI Safety and Fairness
    af_raw = raw.get("ai_fairness") or {}
    ai_fairness = AIFairnessConfig(
        data_limitations=_str(af_raw.get("data_limitations")),
        data_biases=_str(af_raw.get("data_biases")),
        personal_sensitive_information=_str(
            af_raw.get("personal_sensitive_information")
        ),
        data_use_cases=_str(af_raw.get("data_use_cases")),
        data_social_impact=_str(af_raw.get("data_social_impact")),
        has_synthetic_data=bool(af_raw["has_synthetic_data"])
        if "has_synthetic_data" in af_raw
        else None,
    )

    # Lineage
    ln_raw = raw.get("lineage") or {}

    source_datasets = [
        SourceDataset(
            url=str(s.get("url", "")),
            id=_str(s.get("id")),
            name=_str(s.get("name")),
            organisation=_str(s.get("organisation")),
            license=_str(s.get("license")),
        )
        for s in (ln_raw.get("source_datasets") or [])
        if s.get("url")
    ]

    models = [
        ModelRef(
            url=str(m.get("url", "")),
            id=_str(m.get("id")),
            name=_str(m.get("name")),
        )
        for m in (ln_raw.get("models") or [])
        if m.get("url")
    ]

    lineage = LineageConfig(source_datasets=source_datasets, models=models)

    # Activities
    activities = []
    for act_raw in raw.get("activities") or []:
        agents = [
            Agent(
                name=str(a.get("name", "")),
                url=_str(a.get("url")),
                description=_str(a.get("description")),
                is_synthetic=bool(a.get("is_synthetic", False)),
            )
            for a in (act_raw.get("agents") or [])
            if a.get("name")
        ]

        platforms = [
            Platform(
                name=str(p.get("name", "")),
                url=_str(p.get("url")),
                description=_str(p.get("description")),
            )
            for p in (act_raw.get("platforms") or [])
            if p.get("name")
        ]

        collection_types = [
            str(t).strip() for t in (act_raw.get("collection_types") or []) if t
        ]

        activities.append(
            Activity(
                id=str(act_raw.get("id", "")),
                type=str(act_raw.get("type", "")),
                description=_str(act_raw.get("description")),
                start_at=_str(act_raw.get("start_at")),
                end_at=_str(act_raw.get("end_at")),
                collection_types=collection_types,
                agents=agents,
                platforms=platforms,
            )
        )

    return RAIConfig(
        ai_fairness=ai_fairness,
        lineage=lineage,
        activities=activities,
    )
