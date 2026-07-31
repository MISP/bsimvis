import os
import re
import glob

js_dir = 'bsimvis/app/static/js'
js_files = glob.glob(f'{js_dir}/**/*.js', recursive=True)

color_map = {
    '#000': 'var(--window-tray)',
    '#000000': 'var(--window-tray)',
    '#111': 'var(--meta-header-bg)',
    '#111111': 'var(--meta-header-bg)',
    '#121212': 'var(--bg)',
    '#1a1a1a': 'var(--meta-bg)',
    '#1e1e1e': 'var(--card-bg)',
    '#222': 'var(--meta-header-bg)',
    '#222222': 'var(--meta-header-bg)',
    '#333': 'var(--border)',
    '#333333': 'var(--border)',
    '#444': 'var(--border)',
    '#444444': 'var(--border)',
    '#555': 'var(--subtle)',
    '#eee': 'var(--meta-text)',
    '#fff': 'var(--text)',
    '#ffffff': 'var(--text)'
}

for fpath in js_files:
    with open(fpath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # We will replace these specific hex colors when they look like color values.
    # It's inside js files, often in style="..." or .attr("fill", "...")
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
    
    # Also replace rgba(0,0,0,0.85) etc with solid or var bg in tooltips
    new_content = re.sub(r'rgba\(0,\s*0,\s*0,\s*0\.[89]\d*\)', 'var(--window-bg)', new_content)
    new_content = re.sub(r'rgba\(26,\s*26,\s*26,\s*0\.[89]\d*\)', 'var(--card-bg)', new_content)
    
    if new_content != content:
        with open(fpath, 'w', encoding='utf-8') as f:
            f.write(new_content)
