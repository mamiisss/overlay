# Overlay Asset Pipeline

This folder contains the first runtime-friendly morph asset pipeline.

## Convert RBXMX To Descriptor JSON

```powershell
node work\morph-importer\convert-rbxmx-to-overlay-json.js `
  --input work\morph-importer\output\sanitized `
  --output work\morph-importer\output\descriptors
```

Output:

- `descriptors/catalog.json`
- `descriptors/morph_*.json`

The descriptor format is `overlay_json_v1`. It keeps only safe render data:
parts, mesh references, particles, highlights, attachments, decals, textures,
and lights. Scripts and remotes should already be stripped by the sanitizer.

## GitHub Test Flow

Put the generated `catalog.json` and `morph_*.json` files in the same GitHub
folder. Before running the bundled loader in the executor, set:

```lua
getgenv().OverlayAssetCatalogUrl = "https://raw.githubusercontent.com/<user>/<repo>/<commit-or-branch>/descriptors/catalog.json"
```

Then run:

```lua
github_runtime_ui_loader_bundled.lua
```

The runtime will:

1. Load the asset catalog.
2. Show catalog assets in the Morph catalog dropdown.
3. Register the selected asset with the backend on first use.
4. Download and cache the descriptor JSON.
5. Build client-only overlay instances from the descriptor.

## Current Scope

This is the first vertical slice. It supports the descriptor path and basic
native/proxy binding. Complex full-body morphs may need more property mappings,
part limits, and cleanup rules after executor testing.
