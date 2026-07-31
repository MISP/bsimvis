import glob

files = [
    'bsimvis/app/static/function/features/index.html',
    'bsimvis/app/static/function/index.html',
    'bsimvis/app/static/feature/index.html',
    'bsimvis/app/static/js/code_renderer.js',
    'bsimvis/app/static/js/views/function_view.js'
]

for filepath in files:
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # In JS files like code_renderer.js, options.bg || '#272822' becomes options.bg || 'var(--card-bg)'
    new_content = content.replace('#272822', 'var(--card-bg)')
    
    if new_content != content:
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(new_content)
        print(f"Fixed {filepath}")
