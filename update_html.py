import os
import re

fpath = 'bsimvis/app/static/file/index.html'

color_map = {
    '#000': 'var(--bg)', # Changed from window-tray since it's an input background
    '#000000': 'var(--bg)',
    '#111': 'var(--meta-header-bg)',
    '#1a1a1a': 'var(--meta-bg)',
    '#1e1e1e': 'var(--card-bg)',
    '#222': 'var(--meta-header-bg)',
    '#333': 'var(--border)',
    '#444': 'var(--border)',
    '#ccc': 'var(--meta-text-muted)',
    '#eee': 'var(--meta-text)',
    '#fff': 'var(--text)',
    '#ffffff': 'var(--text)',
    '#f92672': 'var(--token-instruction)'
}

with open(fpath, 'r', encoding='utf-8') as f:
    content = f.read()

def replace_hex(match):
    hex_val = match.group(1).lower()
    if hex_val in color_map:
        return color_map[hex_val]
    if len(hex_val) == 4:
        expanded = f"#{hex_val[1]*2}{hex_val[2]*2}{hex_val[3]*2}"
        if expanded in color_map:
            return color_map[expanded]
    return match.group(0)

# regex replace hex colors
new_content = re.sub(r'(#[0-9a-fA-F]{3,6})(?![a-zA-Z0-9_-])', replace_hex, content)

# Also fix the array of colors for D3
new_content = new_content.replace(
    "['#66d9ef', '#a6e22e', '#f92672', '#fd971f', '#ae81ff', '#e6db74', '#75715e']",
    "['var(--token-register)', 'var(--token-symbol)', 'var(--token-instruction)', 'var(--token-warning)', 'var(--token-address)', 'var(--token-constant)', 'var(--token-comment)']"
)

if new_content != content:
    with open(fpath, 'w', encoding='utf-8') as f:
        f.write(new_content)
