#!/bin/bash
# Download the full Open Targets Platform dataset and the human-authored
# Croissant ground truth for external evaluation of Croissant Baker.
#
# Downloads all Parquet partitions for each of the 55 datasets.
# Total size is approximately 20-30 GB depending on the current release.
#
# Usage:
#   bash eval/open_targets/download.sh
#
# The downloaded Parquet directories are git-ignored; only this script and
# croissant_ground_truth.json are committed.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
BASE_URL="https://ftp.ebi.ac.uk/pub/databases/opentargets/platform/latest/output"

# ---------- ground truth ----------
GT_URL="https://ftp.ebi.ac.uk/pub/databases/opentargets/platform/latest/croissant.json"
GT_DEST="$SCRIPT_DIR/croissant_ground_truth.json"
if [ ! -f "$GT_DEST" ]; then
  echo "Downloading Open Targets ground truth croissant.json ..."
  curl -sfL -o "$GT_DEST" "$GT_URL"
  echo "  saved to $GT_DEST"
else
  echo "Ground truth already present."
fi

# ---------- datasets ----------
DATASETS=(
  association_by_datasource_direct
  association_by_datasource_indirect
  association_by_datatype_direct
  association_by_datatype_indirect
  association_overall_direct
  association_overall_indirect
  biosample
  clinical_indication
  clinical_report
  clinical_target
  colocalisation
  credible_set
  disease
  disease_hpo
  disease_phenotype
  drug_mechanism_of_action
  drug_molecule
  drug_warning
  enhancer_to_gene
  evidence_cancer_biomarkers
  evidence_cancer_gene_census
  evidence_clingen
  evidence_clinical_precedence
  evidence_crispr
  evidence_crispr_screen
  evidence_europepmc
  evidence_eva
  evidence_eva_somatic
  evidence_expression_atlas
  evidence_gene2phenotype
  evidence_gene_burden
  evidence_genomics_england
  evidence_gwas_credible_sets
  evidence_impc
  evidence_intogen
  evidence_orphanet
  evidence_reactome
  evidence_uniprot_literature
  evidence_uniprot_variants
  expression
  go
  interaction
  interaction_evidence
  l2g_prediction
  literature
  literature_vector
  mouse_phenotype
  openfda_significant_adverse_drug_reactions
  pharmacogenomics
  so
  study
  target
  target_essentiality
  target_prioritisation
  variant
)

echo ""
echo "Downloading all Parquet partitions for ${#DATASETS[@]} datasets..."
echo "(This may take a while depending on your connection.)"
echo ""

total_files=0
for ds in "${DATASETS[@]}"; do
  dest_dir="$SCRIPT_DIR/$ds"
  mkdir -p "$dest_dir"

  # List all .parquet files from the FTP index
  all_files=$(curl -sfL "$BASE_URL/$ds/" \
    | rg -o 'href="([^"]*\.parquet)"' -r '$1' || true)

  if [ -z "$all_files" ]; then
    echo "  [WARN] $ds: no parquet files found, skipping"
    continue
  fi

  n_total=$(echo "$all_files" | wc -l | tr -d ' ')
  n_existing=$(ls "$dest_dir"/*.parquet 2>/dev/null | wc -l | tr -d ' ')

  if [ "$n_existing" -ge "$n_total" ]; then
    echo "  [skip] $ds ($n_existing/$n_total partitions already present)"
    total_files=$((total_files + n_existing))
    continue
  fi

  echo "  $ds: downloading $n_total partitions ($n_existing already present)..."
  while IFS= read -r fname; do
    if [ -f "$dest_dir/$fname" ]; then
      continue
    fi
    curl -sfL -o "$dest_dir/$fname" "$BASE_URL/$ds/$fname"
    total_files=$((total_files + 1))
  done <<< "$all_files"
done

echo ""
echo "Download complete. $total_files total Parquet files."
du -sh "$SCRIPT_DIR"
