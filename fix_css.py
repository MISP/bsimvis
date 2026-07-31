import re
import os

with open('bsimvis/app/static/css/style.css', 'r', encoding='utf-8') as f:
    content = f.read()

root_vars = """:root {
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
}"""

light_vars = """html.light-theme {
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
}"""

# Replace the block for :root
content = re.sub(r':root\s*\{[^}]*\}', root_vars, content, count=1)
# Replace the block for html.light-theme
content = re.sub(r'html\.light-theme\s*\{[^}]*\}', light_vars, content, count=1)

with open('bsimvis/app/static/css/style.css', 'w', encoding='utf-8') as f:
    f.write(content)
