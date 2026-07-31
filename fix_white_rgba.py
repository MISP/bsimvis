import re
import glob

# Collect all js and css files
files = glob.glob('bsimvis/app/static/js/**/*.js', recursive=True) + \
        glob.glob('bsimvis/app/static/css/**/*.css', recursive=True) + \
        glob.glob('bsimvis/app/static/file/**/*.html', recursive=True)

for fpath in files:
    with open(fpath, 'r', encoding='utf-8') as f:
        content = f.read()

    new_content = content
    
    # Let's extract the root and light-theme blocks temporarily
    root_match = re.search(r':root\s*\{[^}]*\}', new_content)
    light_match = re.search(r'html\.light-theme\s*\{[^}]*\}', new_content)
    
    root_block = root_match.group(0) if root_match else None
    light_block = light_match.group(0) if light_match else None
    
    if root_block:
        new_content = new_content.replace(root_block, '___ROOT_BLOCK___')
    if light_block:
        new_content = new_content.replace(light_block, '___LIGHT_BLOCK___')

    # Replace hardcoded translucent whites
    # In borders, rgba(255,255,255,...) should be var(--border)
    new_content = re.sub(r'border(-[a-z]+)?\s*:\s*([^;]*?)rgba\(\s*255\s*,\s*255\s*,\s*255\s*,\s*0\.[0-9]+\s*\)', r'border\1: \2var(--border)', new_content)
    new_content = re.sub(r'border-color\s*:\s*rgba\(\s*255\s*,\s*255\s*,\s*255\s*,\s*0\.[0-9]+\s*\)', r'border-color: var(--border)', new_content)
    
    # In backgrounds, rgba(255,255,255,...) should be var(--hover) or var(--card-bg) or var(--meta-bg) depending on opacity.
    # High opacity (>0.1) -> var(--meta-header-bg)
    new_content = re.sub(r'background(?:-color)?\s*:\s*rgba\(\s*255\s*,\s*255\s*,\s*255\s*,\s*0\.[1-9][0-9]*\s*\)', r'background: var(--meta-header-bg)', new_content)
    # Low opacity (<=0.1) -> var(--card-bg) or hover
    new_content = re.sub(r'background(?:-color)?\s*:\s*rgba\(\s*255\s*,\s*255\s*,\s*255\s*,\s*0\.0[0-9]*\s*\)', r'background: var(--hover)', new_content)
    
    if root_block:
        new_content = new_content.replace('___ROOT_BLOCK___', root_block)
    if light_block:
        new_content = new_content.replace('___LIGHT_BLOCK___', light_block)

    # Any remaining rgba(255,255,255,...) just to be safe
    new_content = re.sub(r'rgba\(\s*255\s*,\s*255\s*,\s*255\s*,\s*0\.[0-9]+\s*\)', r'var(--border)', new_content)
    # Any remaining rgba(0,0,0,...) just to be safe
    new_content = re.sub(r'rgba\(\s*0\s*,\s*0\s*,\s*0\s*,\s*0\.[0-9]+\s*\)', r'var(--border)', new_content)

    if new_content != content:
        with open(fpath, 'w', encoding='utf-8') as f:
            f.write(new_content)
