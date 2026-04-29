# SPECT demo (mixed DICOM + NIfTI subset)

A small mixed-format fixture used by `tests/test_end_to_end.py`. Layout
mirrors the source dataset's own directory structure so a single bake
exercises the DICOM and NIfTI handlers concurrently.

```
spect_demo/
├── DICOM/   3 multi-frame SPECT acquisitions (.dcm, ~1.4 MB)
└── NIfTI/   3 segmentation masks aligned with those scans (.nii.gz, ~5 KB)
```

## Source

Subset of the **Myocardial Perfusion Scintigraphy Image Database** (open
access, CC-BY-4.0):

- Citation: Calixto, R. et al. (2025). Myocardial Perfusion Scintigraphy
  Image Database (version 1.0.0). PhysioNet.
- Landing page: https://physionet.org/content/myocardial-perfusion-spect/1.0.0/
- DOI: https://doi.org/10.13026/c2tw-tx28

The full dataset bundles 103 DICOMs and 100 NIfTI masks. Three of each
were copied here to keep the fixture under 2 MB while exercising the
real-world traits both handlers need to handle:

DICOM (Nuclear Medicine Image Storage SOP class):
- multi-frame (`NumberOfFrames=50`)
- modality `NM`
- distinct PatientID, StudyInstanceUID, SeriesInstanceUID per file
- 70x70 pixels, 16 bits/pixel
- anonymised per DICOM PS3.15 Annex E

NIfTI (segmentation masks aligned with the SPECT volumes):
- gzip-compressed (`.nii.gz`)
- 3D (70x70x50 voxels, no time axis)
- `int16` data type
- voxel spacing 4x4x4 mm
