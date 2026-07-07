# Overlay Runtime Test Assets

Upload this folder to GitHub.

Recommended layout:

- descriptors/catalog.json
- descriptors/morph_*.json
- runtime/github_runtime_ui_loader_bundled.lua

Executor setup before running the bundled loader:

```lua
getgenv().OverlayAssetCatalogUrl = "https://raw.githubusercontent.com/<user>/<repo>/<branch>/descriptors/catalog.json"
```

Then run the raw bundled loader from:

```text
https://raw.githubusercontent.com/<user>/<repo>/<branch>/runtime/github_runtime_ui_loader_bundled.lua
```

Use the Morph catalog dropdown in the UI, select a morph, then Apply morph.
