import os
import re
import glob

# Collect all js and css files
files = glob.glob('bsimvis/app/static/js/**/*.js', recursive=True) + glob.glob('bsimvis/app/static/css/**/*.css', recursive=True) + glob.glob('bsimvis/app/static/file/**/*.html', recursive=True)

color_map = {
    '#0a0a0a': 'var(--bg)',
    '#0f0f0f': 'var(--card-bg)',
    '#252525': 'var(--meta-bg)',
    '#1a1a1a': 'var(--meta-bg)',
    '#1e1e1e': 'var(--card-bg)',
    '#111': 'var(--meta-header-bg)',
    '#111111': 'var(--meta-header-bg)',
    '#222': 'var(--meta-header-bg)',
    '#222222': 'var(--meta-header-bg)',
    '#333': 'var(--border)',
    '#333333': 'var(--border)',
    '#444': 'var(--border)',
    '#444444': 'var(--border)',
    '#555': 'var(--subtle)',
    '#555555': 'var(--subtle)',
    '#666': 'var(--subtle)',
    '#666666': 'var(--subtle)',
    '#888': 'var(--subtle)',
    '#888888': 'var(--subtle)',
    '#aaa': 'var(--meta-text-muted)',
    '#aaaaaa': 'var(--meta-text-muted)',
    '#ccc': 'var(--meta-text-muted)',
    '#cccccc': 'var(--meta-text-muted)',
    '#eee': 'var(--meta-text)',
    '#eeeeee': 'var(--meta-text)',
    '#fff': 'var(--text)',
    '#ffffff': 'var(--text)',
    '#0d0f14': 'var(--window-bg)',
    '#1a1f29': 'var(--window-header)',
    '#050505': 'var(--window-tray)',
    '#0a0c10': 'var(--window-bg)',
    '#000': 'var(--window-tray)',
    '#000000': 'var(--window-tray)',
}

def replace_hex(match):
    hex_val = match.group(1).lower()
    if hex_val in color_map:
        return color_map[hex_val]
    if len(hex_val) == 4:
        expanded = f"#{hex_val[1]*2}{hex_val[2]*2}{hex_val[3]*2}"
        if expanded in color_map:
            return color_map[expanded]
    return match.group(0)

# Replace RGBA colors that are "flashy" with color-mix
rgba_map = {
    # green
    r'rgba\(\s*166\s*,\s*226\s*,\s*46\s*,\s*0\.([0-9]+)\s*\)': lambda m: f'color-mix(in srgb, var(--token-symbol) {int(float("0."+m.group(1))*100)}%, transparent)',
    # blue
    r'rgba\(\s*102\s*,\s*217\s*,\s*239\s*,\s*0\.([0-9]+)\s*\)': lambda m: f'color-mix(in srgb, var(--token-register) {int(float("0."+m.group(1))*100)}%, transparent)',
    # pink
    r'rgba\(\s*249\s*,\s*38\s*,\s*114\s*,\s*0\.([0-9]+)\s*\)': lambda m: f'color-mix(in srgb, var(--token-instruction) {int(float("0."+m.group(1))*100)}%, transparent)',
    # orange
    r'rgba\(\s*253\s*,\s*151\s*,\s*31\s*,\s*0\.([0-9]+)\s*\)': lambda m: f'color-mix(in srgb, var(--token-warning) {int(float("0."+m.group(1))*100)}%, transparent)',
    # purple
    r'rgba\(\s*174\s*,\s*129\s*,\s*255\s*,\s*0\.([0-9]+)\s*\)': lambda m: f'color-mix(in srgb, var(--token-address) {int(float("0."+m.group(1))*100)}%, transparent)',
    # dark rgba background
    r'rgba\(\s*0\s*,\s*0\s*,\s*0\s*,\s*0\.([89][0-9]*)\s*\)': 'var(--window-bg)',
    r'rgba\(\s*18\s*,\s*18\s*,\s*18\s*,\s*0\.([89][0-9]*)\s*\)': 'var(--window-bg)',
    r'rgba\(\s*26\s*,\s*26\s*,\s*26\s*,\s*0\.([89][0-9]*)\s*\)': 'var(--card-bg)',
    r'rgba\(\s*30\s*,\s*30\s*,\s*30\s*,\s*0\.([89][0-9]*)\s*\)': 'var(--card-bg)',
    r'rgba\(\s*40\s*,\s*40\s*,\s*40\s*,\s*0\.([89][0-9]*)\s*\)': 'var(--hover)',
    r'rgba\(\s*20\s*,\s*22\s*,\s*26\s*,\s*0\.([89][0-9]*)\s*\)': 'var(--meta-bg)',
}

for fpath in files:
    with open(fpath, 'r', encoding='utf-8') as f:
        content = f.read()

    new_content = content
    # For JS files we can just replace all hex colors that match
    # For CSS we do the same, EXCEPT inside :root and .light-theme
    
    # Let's extract the root and light-theme blocks temporarily to prevent messing them up
    root_match = re.search(r':root\s*\{[^}]*\}', new_content)
    light_match = re.search(r'html\.light-theme\s*\{[^}]*\}', new_content)
    
    root_block = root_match.group(0) if root_match else None
    light_block = light_match.group(0) if light_match else None
    
    if root_block:
        new_content = new_content.replace(root_block, '___ROOT_BLOCK___')
    if light_block:
        new_content = new_content.replace(light_block, '___LIGHT_BLOCK___')

    new_content = re.sub(r'(#[0-9a-fA-F]{3,6})(?![a-zA-Z0-9_-])', replace_hex, new_content)
    
    for pattern, repl in rgba_map.items():
        new_content = re.sub(pattern, repl, new_content)
        
    if root_block:
        new_content = new_content.replace('___ROOT_BLOCK___', root_block)
    if light_block:
        new_content = new_content.replace('___LIGHT_BLOCK___', light_block)

    if new_content != content:
        with open(fpath, 'w', encoding='utf-8') as f:
            f.write(new_content)
