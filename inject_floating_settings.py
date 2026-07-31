import os

utils_path = 'bsimvis/app/static/js/utils.js'

with open(utils_path, 'r', encoding='utf-8') as f:
    content = f.read()

injection = """
// Inject floating UI Settings button for standalone views
document.addEventListener('DOMContentLoaded', () => {
    // Only inject if the page doesn't have the main dashboard settings button
    if (!document.getElementById('header-settings-btn') && !document.getElementById('floating-settings-btn')) {
        const btn = document.createElement('button');
        btn.id = 'floating-settings-btn';
        btn.innerHTML = '<i class="fa-solid fa-sliders"></i>';
        btn.title = "UI Settings";
        btn.style.cssText = "position:fixed; bottom:20px; right:20px; z-index:9999; background:var(--card-bg); color:var(--accent); border:1px solid var(--border); border-radius:50%; width:45px; height:45px; cursor:pointer; box-shadow:0 4px 12px rgba(0,0,0,0.3); display:flex; align-items:center; justify-content:center; font-size:1.2rem; transition: all 0.2s;";
        
        btn.onmouseover = () => btn.style.transform = "scale(1.1)";
        btn.onmouseout = () => btn.style.transform = "scale(1)";
        
        btn.onclick = () => {
            let panel = document.getElementById('floating-ui-settings');
            if (!panel) {
                panel = document.createElement('div');
                panel.id = 'floating-ui-settings';
                panel.style.cssText = "position:fixed; bottom:75px; right:20px; z-index:9999; background:var(--card-bg); border:1px solid var(--border); border-radius:8px; padding:20px; width:280px; box-shadow:0 4px 15px rgba(0,0,0,0.5); color:var(--text); display:block; font-family: 'Inter', sans-serif;";
                
                const isLight = document.documentElement.classList.contains('light-theme');
                const useFloating = localStorage.getItem('useFloatingWindows') === 'true';
                const includeHeaders = localStorage.getItem('includeHeaders') === 'true';
                
                panel.innerHTML = `
                    <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:15px;">
                        <h3 style="margin:0; font-size:1rem; color:var(--accent);"><i class="fa-solid fa-sliders"></i> UI Settings</h3>
                        <button onclick="document.getElementById('floating-ui-settings').style.display='none'" style="background:none; border:none; color:var(--dim); cursor:pointer;"><i class="fa-solid fa-times"></i></button>
                    </div>
                    <div style="display:flex; align-items:center; gap:10px; margin-bottom: 20px;">
                        <input type="checkbox" id="floating-param-light-theme" ${isLight ? 'checked' : ''} onchange="
                            if(this.checked) {
                                document.documentElement.classList.add('light-theme');
                                localStorage.setItem('lightTheme', 'true');
                            } else {
                                document.documentElement.classList.remove('light-theme');
                                localStorage.setItem('lightTheme', 'false');
                            }
                        ">
                        <label style="font-size:0.8rem; cursor:pointer;" onclick="document.getElementById('floating-param-light-theme').click()">Light Theme</label>
                    </div>
                `;
                document.body.appendChild(panel);
            } else {
                panel.style.display = panel.style.display === 'none' ? 'block' : 'none';
            }
        };
        document.body.appendChild(btn);
    }
});
"""

if "floating-settings-btn" not in content:
    with open(utils_path, 'a', encoding='utf-8') as f:
        f.write(injection)
    print("Injected floating settings button to utils.js")
else:
    print("Already injected.")
