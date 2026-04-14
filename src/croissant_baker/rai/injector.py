"""Inject RAI and PROV-O attributes into a Croissant JSON-LD metadata dict."""

from __future__ import annotations

from croissant_baker.rai.schema import Activity, RAIConfig

_PROV_NS = "http://www.w3.org/ns/prov#"

_ACTIVITY_LABELS = {
    "data_collection": "Data Collection",
    "data_annotation": "Data Annotation",
    "data_preprocessing": "Data Preprocessing",
}


def inject_rai(metadata: dict, config: RAIConfig) -> dict:
    """
    Inject RAI and PROV-O attributes into a Croissant metadata dict.

    Mutates and returns the dict. Fields that are None/empty are skipped.
    The prov: namespace is added to @context automatically when needed.

    Structure:
    - AI Safety and Fairness fields are direct rai: properties on the dataset.
    - Source datasets → prov:wasDerivedFrom.
    - Models that used this dataset → rai:usedBy.
    - Activities → prov:wasGeneratedBy (list of prov:Activity), each with
      optional prov:wasAssociatedWith (agents) and rai:usedPlatform (platforms).
    """
    _ensure_prov_context(metadata, config)

    # AI Safety and Fairness
    af = config.ai_fairness
    if af.data_limitations:
        metadata["rai:dataLimitations"] = af.data_limitations
    if af.data_bias:
        metadata["rai:dataBias"] = af.data_bias
    if af.personal_sensitive_information:
        metadata["rai:personalSensitiveInformation"] = af.personal_sensitive_information
    if af.data_use_cases:
        metadata["rai:dataUseCases"] = af.data_use_cases
    if af.social_impact:
        metadata["rai:socialImpact"] = af.social_impact
    if af.has_synthetic_data is not None:
        metadata["rai:hasSyntheticData"] = af.has_synthetic_data

    # Lineage — source datasets
    if config.lineage.source_datasets:
        metadata["prov:wasDerivedFrom"] = [
            _build_source_dataset(s) for s in config.lineage.source_datasets
        ]

    # Lineage — models that used this dataset
    if config.lineage.models:
        metadata["rai:usedBy"] = [
            {
                k: v
                for k, v in {
                    "url": m.url,
                    "id": m.id,
                    "name": m.name,
                }.items()
                if v
            }
            for m in config.lineage.models
        ]

    # Activities
    activities = [_build_activity(act) for act in config.activities]
    if activities:
        metadata["prov:wasGeneratedBy"] = (
            activities[0] if len(activities) == 1 else activities
        )

    return metadata


def _build_source_dataset(s) -> dict:
    node: dict = {
        k: v
        for k, v in {
            "url": s.url,
            "id": s.id,
            "name": s.name,
            "license": s.license,
        }.items()
        if v
    }
    if s.organisation:
        node["prov:wasAssociatedWith"] = {
            "@type": "prov:Organization",
            "name": s.organisation,
        }
    return node


def _build_activity(act: Activity) -> dict:
    label = _ACTIVITY_LABELS.get(act.type, act.type)
    node: dict = {
        "@type": "prov:Activity",
        "@id": act.id,
        "prov:label": label,
        "prov:type": label,
    }

    if act.description:
        node["prov:description"] = act.description
    if act.start_at:
        node["prov:startedAtTime"] = act.start_at
    if act.end_at:
        node["prov:endedAtTime"] = act.end_at

    if act.agents:
        agent_nodes = []
        for a in act.agents:
            agent_type = "prov:SoftwareAgent" if a.is_synthetic else "prov:Agent"
            agent: dict = {"@type": agent_type, "name": a.name}
            if a.url:
                agent["url"] = a.url
            if a.description:
                agent["prov:description"] = a.description
            agent_nodes.append(agent)
        node["prov:wasAssociatedWith"] = (
            agent_nodes[0] if len(agent_nodes) == 1 else agent_nodes
        )

    if act.platforms:
        platform_nodes = []
        for p in act.platforms:
            plat: dict = {"name": p.name}
            if p.url:
                plat["url"] = p.url
            if p.description:
                plat["prov:description"] = p.description
            platform_nodes.append(plat)
        node["rai:usedPlatform"] = (
            platform_nodes[0] if len(platform_nodes) == 1 else platform_nodes
        )

    return node


def _ensure_prov_context(metadata: dict, config: RAIConfig) -> None:
    """Add prov: namespace to @context if any PROV-O output will be injected."""
    needs_prov = bool(config.activities or config.lineage.source_datasets)
    if not needs_prov:
        return
    ctx = metadata.get("@context")
    if isinstance(ctx, dict) and "prov" not in ctx:
        ctx["prov"] = _PROV_NS
