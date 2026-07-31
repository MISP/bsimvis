import re
import os

files = ['bsimvis/app/static/css/style.css', 'bsimvis/app/static/css/dashboard.css']

# We need to map hardcoded colors to var(...)
# First, let's collect exactly what is in root:
root_vars = """
    color-scheme: dark;
    --bg: #121212;
    --card-bg: #1e1e1e;
    --text: #f8f8f2;
    --accent: #66d9ef;
    --subtle: #75715e;
    --success: #a6e22e;
    --info: #ae81ff;
    --border: #3e3d32;
    --hover: #2d2d2d;
    --dim: #75715e;

    /* Semantic UI */
    --meta-bg: #1a1a1a;
    --meta-header-bg: #222222;
    --meta-border: #333333;
    --meta-text: #eeeeee;
    --meta-text-muted: #aaaaaa;
    --window-bg: #0d0f14;
    --window-header: #1a1f29;
    --window-tray: #050505;
    
    /* Tokens */
    --token-instruction: #f92672;
    --token-register: #66d9ef;
    --token-address: #ae81ff;
    --token-constant: #ae81ff;
    --token-symbol: #a6e22e;
    --token-comment: #75715e;
    --token-label: #a6e22e;
    --token-operand: #f8f8f2;
    --token-warning: #fd971f;
"""

light_vars = """
    color-scheme: light;
    --bg: #ffffff;
    --card-bg: #f7f7f7;
    --text: #1a1a1a;
    --accent: #0066cc;
    --subtle: #666666;
    --success: #2e7d32;
    --info: #6a1b9a;
    --border: #dddddd;
    --hover: #e8e8e8;
    --dim: #777777;

    /* Semantic UI */
    --meta-bg: #ffffff;
    --meta-header-bg: #f0f0f0;
    --meta-border: #cccccc;
    --meta-text: #333333;
    --meta-text-muted: #666666;
    --window-bg: #ffffff;
    --window-header: #eeeeee;
    --window-tray: #e0e0e0;
    
    /* Tokens */
    --token-instruction: #d1124f;
    --token-register: #0066cc;
    --token-address: #6a1b9a;
    --token-constant: #6a1b9a;
    --token-symbol: #2e7d32;
    --token-comment: #666666;
    --token-label: #2e7d32;
    --token-operand: #1a1a1a;
    --token-warning: #c25e00;
"""

color_map = {
    # tokens
    '#f92672': 'var(--token-instruction)',
    '#66d9ef': 'var(--token-register)',
    '#ae81ff': 'var(--token-address)',
    '#a6e22e': 'var(--token-symbol)',
    '#fd971f': 'var(--token-warning)',
    
    # meta UI dark colors
    '#1a1a1a': 'var(--meta-bg)',
    '#222': 'var(--meta-header-bg)',
    '#222222': 'var(--meta-header-bg)',
    '#333': 'var(--meta-border)',
    '#333333': 'var(--meta-border)',
    '#444': 'var(--border)',
    '#444444': 'var(--border)',
    '#eee': 'var(--meta-text)',
    '#eeeeee': 'var(--meta-text)',
    '#ccc': 'var(--meta-text-muted)',
    '#cccccc': 'var(--meta-text-muted)',
    '#aaa': 'var(--meta-text-muted)',
    '#aaaaaa': 'var(--meta-text-muted)',
    '#ddd': 'var(--meta-text)',
    '#dddddd': 'var(--meta-text)',
    '#555': 'var(--subtle)',
    '#555555': 'var(--subtle)',
    '#666': 'var(--subtle)',
    '#666666': 'var(--subtle)',
    '#777': 'var(--subtle)',
    '#777777': 'var(--subtle)',
    '#888': 'var(--subtle)',
    '#888888': 'var(--subtle)',
    
    # Specific backgrounds
    '#0d0f14': 'var(--window-bg)',
    '#1a1f29': 'var(--window-header)',
    '#050505': 'var(--window-tray)',
    '#000': 'var(--window-tray)',
    '#000000': 'var(--window-tray)',
    '#111': 'var(--meta-header-bg)',
    '#111111': 'var(--meta-header-bg)',
    '#0a0c10': 'var(--window-bg)',
    
    # Text colors
    '#fff': 'var(--text)',
    '#ffffff': 'var(--text)',
    
    # Legacy
    '#2d2e27': 'var(--meta-bg)',
    '#3e3d32': 'var(--meta-border)',
    '#f8f8f2': 'var(--token-operand)',
    '#75715e': 'var(--token-comment)'
}

for fpath in files:
    with open(fpath, 'r') as f:
        content = f.read()

    # insert vars into :root and html.light-theme in style.css
    if 'style.css' in fpath:
        # replace the ENTIRE :root block and html.light-theme block
        # to ensure we don't duplicate
        root_start = content.find(':root {')
        root_end = content.find('}', root_start)
        
        light_start = content.find('html.light-theme {')
        light_end = content.find('}', light_start)
        
        # We need to replace safely. 
        # First light theme
        if light_start != -1:
            content = content[:light_start] + "html.light-theme {" + light_vars + "}" + content[light_end+1:]
            
        # Then root
        if root_start != -1:
            # Need to find it again because offsets changed
            root_start = content.find(':root {')
            root_end = content.find('}', root_start)
            content = content[:root_start] + ":root {" + root_vars + "}" + content[root_end+1:]

    # regex replace hex colors
    def replace_hex(match):
        hex_val = match.group(1).lower()
        if hex_val in color_map:
            return color_map[hex_val]
        if len(hex_val) == 4:
            expanded = f"#{hex_val[1]*2}{hex_val[2]*2}{hex_val[3]*2}"
            if expanded in color_map:
                return color_map[expanded]
        return match.group(0)

    content = re.sub(r'(#[0-9a-fA-F]{3,6})(?![a-zA-Z0-9_-])', replace_hex, content)
    
    # fix search input text
    content = content.replace("color: var(--meta-header-bg) !important;", "color: var(--text) !important;")
    
    with open(fpath, 'w') as f:
        f.write(content)
