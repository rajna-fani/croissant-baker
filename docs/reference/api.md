# API Reference

Use `croissant-baker` as a Python library to generate Croissant metadata
programmatically — without the CLI.

## MetadataGenerator

::: croissant_baker.metadata_generator.MetadataGenerator
    options:
      members:
        - __init__
        - generate_metadata
        - save_metadata

## File Discovery

::: croissant_baker.files.discover_files

## Handler Interface

::: croissant_baker.handlers.base_handler.FileTypeHandler
    options:
      members:
        - can_handle
        - extract_metadata
        - build_croissant
