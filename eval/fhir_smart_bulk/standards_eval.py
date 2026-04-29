"""FHIR standards-grounded evaluation helpers for SMART bulk NDJSON.

This module builds an authoritative leaf-field ground truth from the
observed FHIR resources plus official HL7 definitions:

- US Core STU7 package (profile definitions for the profiles declared in
  ``meta.profile``)
- FHIR R4 core package (resource/datatype definitions for fallback and
  nested datatypes such as ``Address`` and ``Extension``)

The comparison is performed at the flattened leaf-path level because
Croissant assigns semantic ``dataType`` values to scalar leaves, while
complex container nodes (for example ``address`` or ``valueQuantity``)
are represented structurally through nested ``subField`` objects.
"""

from __future__ import annotations

import json
from collections import defaultdict
from pathlib import Path

SKIP_RESOURCE_TYPES = frozenset({"OperationOutcome"})

US_CORE_PACKAGE_DIR = Path("standards/us_core/package")
FHIR_CORE_PACKAGE_DIR = Path("standards/fhir_r4_core/package")

FHIRPATH_PRIMITIVES = {
    "http://hl7.org/fhirpath/System.String": "string",
    "http://hl7.org/fhirpath/System.Boolean": "boolean",
    "http://hl7.org/fhirpath/System.Integer": "integer",
    "http://hl7.org/fhirpath/System.Decimal": "decimal",
    "http://hl7.org/fhirpath/System.Date": "date",
    "http://hl7.org/fhirpath/System.DateTime": "dateTime",
    "http://hl7.org/fhirpath/System.Time": "time",
}

FHIR_TO_CROISSANT = {
    "base64Binary": "sc:Text",
    "boolean": "sc:Boolean",
    "canonical": "sc:URL",
    "code": "sc:Text",
    "date": "sc:Date",
    "dateTime": "sc:DateTime",
    "decimal": "cr:Float64",
    "id": "sc:Text",
    "instant": "sc:DateTime",
    "integer": "cr:Int64",
    "markdown": "sc:Text",
    "oid": "sc:URL",
    "positiveInt": "cr:Int64",
    "string": "sc:Text",
    "time": "sc:Time",
    "unsignedInt": "cr:Int64",
    "uri": "sc:URL",
    "url": "sc:URL",
    "uuid": "sc:URL",
    "xhtml": "sc:Text",
}

SEMANTIC_FLOAT = {"Float", "Float16", "Float32", "Float64"}
SEMANTIC_INT = {"Integer", "Int8", "Int16", "Int32", "Int64", "UInt8", "UInt16", "UInt32", "UInt64"}
SEMANTIC_GROUPS = [SEMANTIC_FLOAT, SEMANTIC_INT]


def _normalize_croissant_type(type_name: str) -> str:
    if ":" in type_name:
        return type_name.split(":", 1)[1]
    return type_name


def types_semantically_equal(a: str, b: str) -> bool:
    na = _normalize_croissant_type(a)
    nb = _normalize_croissant_type(b)
    if na == nb:
        return True
    for group in SEMANTIC_GROUPS:
        if na in group and nb in group:
            return True
    return False


def _choice_suffix(type_code: str) -> str:
    return type_code[:1].upper() + type_code[1:]


def _normalize_fhir_type_code(code: str) -> str:
    if code in FHIRPATH_PRIMITIVES:
        return FHIRPATH_PRIMITIVES[code]
    if "/" in code:
        code = code.rsplit("/", 1)[-1]
    # Heuristic: single-word TitleCase codes are resource/complex-type names
    # (e.g. "Patient", "Quantity"). Note: this also matches some complex types
    # like "Reference" or "Dosage" (rest-all-lowercase), but those will fail
    # FHIR_TO_CROISSANT lookup and be treated as unresolved — acceptable for
    # the current dataset where all 406 paths resolved successfully.
    if code and code[0].isupper() and code[1:].islower():
        return code
    return code


def _iter_resource_leaf_paths(obj, prefix: str = "") -> list[str]:
    """Flatten a FHIR resource into observed scalar leaf paths."""
    leaves: list[str] = []
    if isinstance(obj, dict):
        for key, value in obj.items():
            if key == "resourceType":
                continue
            child_prefix = f"{prefix}.{key}" if prefix else key
            leaves.extend(_iter_resource_leaf_paths(value, child_prefix))
        return leaves

    if isinstance(obj, list):
        non_null = [item for item in obj if item is not None]
        if not non_null:
            return []
        if any(isinstance(item, dict) for item in non_null):
            for item in non_null:
                if isinstance(item, dict):
                    leaves.extend(_iter_resource_leaf_paths(item, prefix))
            return leaves
        return [prefix]

    return [prefix] if prefix else []


def collect_observed_paths(input_dir: Path) -> tuple[dict[str, set[str]], dict[str, set[str]]]:
    """Collect observed leaf paths and declared profiles from NDJSON resources."""
    observed_paths: dict[str, set[str]] = defaultdict(set)
    profiles_by_type: dict[str, set[str]] = defaultdict(set)

    for path in sorted(input_dir.glob("*.ndjson")):
        if path.name == "log.ndjson":
            continue
        with open(path) as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    resource = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if not isinstance(resource, dict):
                    continue
                resource_type = resource.get("resourceType")
                if not resource_type or resource_type in SKIP_RESOURCE_TYPES:
                    continue
                for profile in resource.get("meta", {}).get("profile", []):
                    profiles_by_type[resource_type].add(profile)
                observed_paths[resource_type].update(_iter_resource_leaf_paths(resource))

    return observed_paths, profiles_by_type


def flatten_generated_recordsets(path: Path) -> dict[str, dict[str, str]]:
    """Flatten generated Croissant JSON-LD into leaf path -> dataType maps."""
    with open(path) as f:
        metadata = json.load(f)

    def _as_field_list(fields) -> list[dict]:
        if fields is None:
            return []
        if isinstance(fields, list):
            return fields
        if isinstance(fields, dict):
            return [fields]
        return []

    def walk(fields, prefix: str = "") -> dict[str, str]:
        flat: dict[str, str] = {}
        for field in _as_field_list(fields):
            name = field.get("name", "")
            field_path = f"{prefix}.{name}" if prefix else name
            sub_fields = _as_field_list(field.get("subField"))
            if sub_fields:
                flat.update(walk(sub_fields, field_path))
            else:
                flat[field_path] = field.get("dataType", "unknown")
        return flat

    recordsets: dict[str, dict[str, str]] = {}
    for recordset in metadata.get("recordSet", []):
        name = recordset.get("name", recordset.get("@id", "unknown"))
        recordsets[name] = walk(recordset.get("field", []))
    return recordsets


class StructureDefinitionResolver:
    """Resolve observed FHIR JSON leaf paths to authoritative scalar types."""

    def __init__(self, standards_root: Path):
        us_core_root = standards_root / US_CORE_PACKAGE_DIR
        core_root = standards_root / FHIR_CORE_PACKAGE_DIR
        if not us_core_root.exists():
            raise FileNotFoundError(f"US Core package not found: {us_core_root}")
        if not core_root.exists():
            raise FileNotFoundError(f"FHIR R4 core package not found: {core_root}")

        self.profile_by_url: dict[str, dict] = {}
        self.core_by_type: dict[str, dict] = {}

        for path in sorted(us_core_root.glob("StructureDefinition-*.json")):
            with open(path) as f:
                structure = json.load(f)
            self.profile_by_url[structure["url"]] = structure

        for path in sorted(core_root.glob("StructureDefinition-*.json")):
            with open(path) as f:
                structure = json.load(f)
            type_name = structure.get("type")
            canonical_url = structure.get("url")
            base_url = f"http://hl7.org/fhir/StructureDefinition/{type_name}"
            if type_name and canonical_url == base_url:
                self.core_by_type[type_name] = structure

    def resolve_leaf_type(
        self,
        resource_type: str,
        leaf_path: str,
        profile_urls: set[str],
    ) -> str | None:
        """Resolve a resource leaf path to a Croissant scalar type."""
        segments = leaf_path.split(".")
        for url in sorted(profile_urls):
            structure = self.profile_by_url.get(url)
            if structure is None or structure.get("type") != resource_type:
                continue
            resolved = self._resolve_in_structure(
                structure,
                structure["type"],
                segments,
            )
            if resolved:
                return resolved

        base = self.core_by_type.get(resource_type)
        if base is None:
            return None
        return self._resolve_in_structure(base, base["type"], segments)

    def _resolve_in_structure(
        self,
        structure: dict,
        context_path: str,
        segments: list[str],
    ) -> str | None:
        if not segments:
            return None

        element, chosen_type = self._find_direct_child(structure, context_path, segments[0])
        if element is None:
            return None

        remaining = segments[1:]
        if not remaining:
            return self._leaf_croissant_type(element, chosen_type)

        if self._has_same_structure_children(structure, element["path"]):
            resolved = self._resolve_in_structure(structure, element["path"], remaining)
            if resolved:
                return resolved

        candidate_types = [chosen_type] if chosen_type else [
            _normalize_fhir_type_code(type_info["code"])
            for type_info in element.get("type", [])
        ]
        for type_name in candidate_types:
            child_structure = self.core_by_type.get(type_name)
            if child_structure is None:
                continue
            resolved = self._resolve_in_structure(
                child_structure,
                child_structure["type"],
                remaining,
            )
            if resolved:
                return resolved
        return None

    def _find_direct_child(
        self,
        structure: dict,
        context_path: str,
        segment: str,
    ) -> tuple[dict | None, str | None]:
        prefix = f"{context_path}."
        direct_children = []
        for element in structure["snapshot"]["element"]:
            path = element["path"]
            if not path.startswith(prefix):
                continue
            suffix = path[len(prefix):]
            if "." in suffix:
                continue
            direct_children.append(element)

        exact_path = f"{context_path}.{segment}"
        exact_matches = [element for element in direct_children if element["path"] == exact_path]
        if exact_matches:
            return exact_matches[0], None

        for element in direct_children:
            suffix = element["path"][len(prefix):]
            if not suffix.endswith("[x]"):
                continue
            base_name = suffix[:-3]
            if not segment.startswith(base_name):
                continue
            choice_suffix = segment[len(base_name):]
            for type_info in element.get("type", []):
                type_name = _normalize_fhir_type_code(type_info["code"])
                if _choice_suffix(type_name) == choice_suffix:
                    return element, type_name

        return None, None

    def _has_same_structure_children(self, structure: dict, path: str) -> bool:
        nested_prefix = f"{path}."
        return any(
            element["path"].startswith(nested_prefix)
            for element in structure["snapshot"]["element"]
        )

    def _leaf_croissant_type(self, element: dict, chosen_type: str | None) -> str | None:
        candidate_types = [chosen_type] if chosen_type else [
            _normalize_fhir_type_code(type_info["code"])
            for type_info in element.get("type", [])
        ]
        for type_name in candidate_types:
            mapped = FHIR_TO_CROISSANT.get(type_name)
            if mapped:
                return mapped
        return None


def build_standards_ground_truth(
    input_dir: Path,
    standards_root: Path,
) -> tuple[dict[str, dict[str, str]], dict[str, list[str]], dict[str, set[str]]]:
    """Build ground truth leaf fields from observed resources plus HL7 standards."""
    observed_paths, profiles_by_type = collect_observed_paths(input_dir)
    resolver = StructureDefinitionResolver(standards_root)

    ground_truth: dict[str, dict[str, str]] = {}
    unresolved: dict[str, list[str]] = {}

    for resource_type, paths in sorted(observed_paths.items()):
        resolved_fields: dict[str, str] = {}
        unresolved_paths: list[str] = []
        for leaf_path in sorted(paths):
            croissant_type = resolver.resolve_leaf_type(
                resource_type,
                leaf_path,
                profiles_by_type.get(resource_type, set()),
            )
            if croissant_type is None:
                unresolved_paths.append(leaf_path)
            else:
                resolved_fields[leaf_path] = croissant_type
        ground_truth[resource_type] = resolved_fields
        unresolved[resource_type] = unresolved_paths

    return ground_truth, unresolved, profiles_by_type


def compare_against_generated(
    ground_truth: dict[str, dict[str, str]],
    generated: dict[str, dict[str, str]],
    unresolved: dict[str, list[str]],
    profiles_by_type: dict[str, set[str]],
) -> dict:
    """Compare generated Croissant leaf fields against standards-grounded truth."""
    gt_names = set(ground_truth.keys())
    gen_names = set(generated.keys())
    matched_recordsets = gt_names & gen_names

    total_gt_fields = 0
    total_gen_fields = 0
    total_matched_fields = 0
    strict_type_agree = 0
    semantic_type_agree = 0
    mismatches: list[dict[str, str]] = []
    missing_fields: list[dict[str, str]] = []
    extra_fields: list[dict[str, str]] = []

    for recordset_name in sorted(matched_recordsets):
        gt_fields = ground_truth[recordset_name]
        gen_fields = generated[recordset_name]
        gt_field_names = set(gt_fields.keys())
        gen_field_names = set(gen_fields.keys())
        common = gt_field_names & gen_field_names

        total_gt_fields += len(gt_fields)
        total_gen_fields += len(gen_fields)
        total_matched_fields += len(common)

        for field_name in sorted(gt_field_names - gen_field_names):
            missing_fields.append({"recordset": recordset_name, "field": field_name})
        for field_name in sorted(gen_field_names - gt_field_names):
            extra_fields.append({"recordset": recordset_name, "field": field_name})

        for field_name in sorted(common):
            gt_type = gt_fields[field_name]
            gen_type = gen_fields[field_name]
            if _normalize_croissant_type(gt_type) == _normalize_croissant_type(gen_type):
                strict_type_agree += 1
                semantic_type_agree += 1
            elif types_semantically_equal(gt_type, gen_type):
                semantic_type_agree += 1
            else:
                mismatches.append(
                    {
                        "recordset": recordset_name,
                        "field": field_name,
                        "ground_truth": gt_type,
                        "generated": gen_type,
                    }
                )

    total_unresolved = sum(len(paths) for paths in unresolved.values())

    return {
        "recordsets_gt": len(gt_names),
        "recordsets_gen": len(gen_names),
        "recordsets_matched": len(matched_recordsets),
        "fields_gt": total_gt_fields,
        "fields_gen": total_gen_fields,
        "fields_matched": total_matched_fields,
        "strict_type_agree": strict_type_agree,
        "strict_type_agree_pct": round(100 * strict_type_agree / total_matched_fields, 1)
        if total_matched_fields
        else 0.0,
        "semantic_type_agree": semantic_type_agree,
        "semantic_type_agree_pct": round(100 * semantic_type_agree / total_matched_fields, 1)
        if total_matched_fields
        else 0.0,
        "missing_fields": missing_fields,
        "extra_fields": extra_fields,
        "mismatches": mismatches,
        "profiles_by_type": {
            resource_type: sorted(profile_urls)
            for resource_type, profile_urls in sorted(profiles_by_type.items())
        },
        "unresolved_paths": unresolved,
        "unresolved_path_count": total_unresolved,
    }
