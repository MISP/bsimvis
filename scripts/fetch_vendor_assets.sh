#!/usr/bin/env bash
# Vendors frontend JS/CSS libs locally so the dashboard works offline.
# Re-run after bumping a version below.
set -e
VENDOR="$(dirname "$0")/../bsimvis/app/static/vendor"
mkdir -p "$VENDOR"/{fontawesome/css,fontawesome/webfonts,marked,iro,d3,d3-sankey}

curl -fL https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/css/all.min.css -o "$VENDOR/fontawesome/css/all.min.css"
for f in fa-brands-400 fa-regular-400 fa-solid-900; do
  for ext in woff2 ttf; do
    curl -fL "https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.5.1/webfonts/$f.$ext" -o "$VENDOR/fontawesome/webfonts/$f.$ext"
  done
done

curl -fL https://cdn.jsdelivr.net/npm/marked@13/marked.min.js -o "$VENDOR/marked/marked.min.js"
curl -fL https://cdn.jsdelivr.net/npm/@jaames/iro@5/dist/iro.min.js -o "$VENDOR/iro/iro.min.js"
curl -fL https://d3js.org/d3.v7.min.js -o "$VENDOR/d3/d3.v7.min.js"
curl -fL https://unpkg.com/d3-sankey@0.12.3/dist/d3-sankey.min.js -o "$VENDOR/d3-sankey/d3-sankey.min.js"

# rulezet-editor (AGPL-3.0): fetched at a pinned commit, never committed here.
# /yara falls back to a plain textarea when it is missing.
mkdir -p "$VENDOR/rulezet-editor"
RZE=2ac5f37004ab2a41a364adeb053579ba16da0974
for f in rulezet-editor.css rulezet-editor.iife.js; do
  curl -fL "https://raw.githubusercontent.com/rulezet/rulezet-editor/$RZE/dist/$f" -o "$VENDOR/rulezet-editor/$f"
done

echo "Vendored assets in $VENDOR"
