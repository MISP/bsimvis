import re

# 1. Update style.css with dynamic lightness variables
css_file = 'bsimvis/app/static/css/style.css'
with open(css_file, 'r', encoding='utf-8') as f:
    css_content = f.read()

root_vars_to_add = """
    /* Dynamic Graph/Tag Colors */
    --color-s-high: 100%;
    --color-l-high: 65%;
    --color-s-med: 80%;
    --color-l-med: 60%;
    --color-s-low: 85%;
    --color-l-low: 60%;
    --color-l-dim: 50%;
"""

light_vars_to_add = """
    /* Dynamic Graph/Tag Colors */
    --color-s-high: 80%;
    --color-l-high: 40%;
    --color-s-med: 70%;
    --color-l-med: 45%;
    --color-s-low: 75%;
    --color-l-low: 45%;
    --color-l-dim: 35%;
"""

if '--color-s-high' not in css_content:
    css_content = re.sub(r'(:root\s*\{)(.*?)(?=\})', r'\1\2' + root_vars_to_add, css_content, flags=re.DOTALL)
    css_content = re.sub(r'(html\.light-theme\s*\{)(.*?)(?=\})', r'\1\2' + light_vars_to_add, css_content, flags=re.DOTALL)
    
    with open(css_file, 'w', encoding='utf-8') as f:
        f.write(css_content)

# 2. Update JS files to use the new CSS variables
import glob
js_files = glob.glob('bsimvis/app/static/js/**/*.js', recursive=True)

for js_path in js_files:
    with open(js_path, 'r', encoding='utf-8') as f:
        js = f.read()
    
    js = js.replace('100%, 65%', 'var(--color-s-high), var(--color-l-high)')
    js = js.replace('80%, 60%', 'var(--color-s-med), var(--color-l-med)')
    js = js.replace('85%, 60%', 'var(--color-s-low), var(--color-l-low)')
    js = js.replace('80%, 50%', 'var(--color-s-med), var(--color-l-dim)')
    js = js.replace('70%, 55%', 'var(--color-s-med), var(--color-l-dim)')
    
    with open(js_path, 'w', encoding='utf-8') as f:
        f.write(js)

# 3. Fix HTML file backgrounds and borders
html_file = 'bsimvis/app/static/file/index.html'
with open(html_file, 'r', encoding='utf-8') as f:
    html = f.read()

html = html.replace('background: rgba(0,0,0,0.2); border: 1px solid rgba(255,255,255,0.05);', 'background: var(--card-bg); border: 1px solid var(--border);')
html = html.replace('background: rgba(255, 255, 255, 0.05);', 'background: var(--hover);')
html = html.replace('background:rgba(255,255,255,0.04);', 'background: var(--hover);')
html = html.replace('background: rgba(255,255,255,0.02);', 'background: var(--hover);')
html = html.replace('background: rgba(0,0,0,0.1);', 'background: var(--meta-header-bg);')
html = html.replace("style=\"background: rgba(255,255,255,0.02);", "style=\"background: var(--card-bg);")
html = html.replace("this.style.background='rgba(255,255,255,0.08)'", "this.style.background='var(--hover)'")
html = html.replace("this.style.background='rgba(255,255,255,0.02)'", "this.style.background='var(--card-bg)'")

with open(html_file, 'w', encoding='utf-8') as f:
    f.write(html)

print("Done")
