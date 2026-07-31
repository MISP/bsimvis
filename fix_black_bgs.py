import re

def fix_file(path, replacements):
    with open(path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    new_content = content
    for old, new in replacements:
        new_content = new_content.replace(old, new)
        
    if new_content != content:
        with open(path, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print(f"Updated {path}")

fix_file('bsimvis/app/static/css/dashboard.css', [
    ('background: #121212;', 'background: var(--card-bg);'),
    ('background: var(--text);', 'background: var(--bg);')
])

fix_file('bsimvis/app/static/css/style.css', [
    ('background: #121212;', 'background: var(--card-bg);'),
    ('background: #272822 !important;', 'background: var(--card-bg) !important;'),
    ('background: var(--text);', 'background: var(--bg);')
])

