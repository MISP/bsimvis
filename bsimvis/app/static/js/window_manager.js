/**
 * WindowManager for BSimVis
 * Manages multiple floating, draggable, and resizable windows.
 */

class WindowManager {
    constructor() {
        this.windows = [];
        this.zIndexBase = 10000;
        this.activeWindow = null;
        this.draggedWindow = null;
        this.resizedWindow = null;
        this.resizeDir = null;
        this.initialX = 0;
        this.initialY = 0;
        this.initialWidth = 0;
        this.initialHeight = 0;
        this.initialLeft = 0;
        this.initialTop = 0;

        this.initGlobalEvents();
    }

    createWindow(title, url, options = {}) {
        const type = options.type || 'generic';
        
        // Reuse active window if it's not sticky and matches type (or we allow cross-type reuse)
        if (this.activeWindow && !this.activeWindow.isSticky && !this.activeWindow.isDiminished) {
            const win = this.activeWindow;
            win.el.querySelector('.window-title').innerText = title;
            win.iframe.src = url;
            win.type = type;
            this.focusWindow(win);
            return win;
        }

        const id = 'win-' + Math.random().toString(36).substr(2, 9);
        const winEl = document.createElement('div');
        winEl.className = 'floating-window';
        winEl.id = id;

        let defaultWidth = 800;
        let defaultHeight = 500;
        
        if (type === 'diff' || type === 'features' || type === 'global-feature') {
            defaultWidth = Math.min(1200, window.innerWidth - 100);
            defaultHeight = Math.min(750, window.innerHeight - 100);
        }
        
        const width = options.width || defaultWidth;
        const height = options.height || defaultHeight;
        
        // Stagger windows
        const offset = this.windows.length * 30;
        const left = options.left || Math.max(250, (window.innerWidth - width) / 2 + offset);
        const top = options.top || Math.max(50, (window.innerHeight - height) / 2 + offset);

        winEl.style.width = width + 'px';
        winEl.style.height = height + 'px';
        winEl.style.left = left + 'px';
        winEl.style.top = top + 'px';

        winEl.innerHTML = `
            <div class="window-header">
                <div class="window-controls-left">
                    <button class="win-btn win-back" title="Back"><i class="fa-solid fa-arrow-left"></i></button>
                    <button class="win-btn win-forward" title="Forward"><i class="fa-solid fa-arrow-right"></i></button>
                    <button class="win-btn win-sticky" title="Toggle Sticky (Always on Top)"><i class="fa-solid fa-thumbtack"></i></button>
                </div>
                <div class="window-title">${title}</div>
                <div class="window-controls-right">
                    <button class="win-btn win-dock-left" title="Dock Left"><i class="fa-solid fa-align-left"></i></button>
                    <button class="win-btn win-dock-bottom" title="Dock Bottom"><i class="fa-solid fa-window-minimize" style="transform: rotate(180deg)"></i></button>
                    <button class="win-btn win-dock-right" title="Dock Right"><i class="fa-solid fa-align-right"></i></button>
                    <button class="win-btn win-diminish" title="Diminish/Restore"><i class="fa-solid fa-window-minimize"></i></button>
                    <button class="win-btn win-popout" title="Open in new window"><i class="fa-solid fa-up-right-from-square"></i></button>
                    <button class="win-btn win-close" title="Close"><i class="fa-solid fa-times"></i></button>
                </div>
            </div>
            <div class="window-content">
                <iframe src="${url}" name="${id}-frame" id="${id}-frame"></iframe>
            </div>
            <div class="win-glass-overlay"></div>
            <!-- Resize handles -->
            <div class="win-resizer win-resizer-n" data-dir="n"></div>
            <div class="win-resizer win-resizer-s" data-dir="s"></div>
            <div class="win-resizer win-resizer-e" data-dir="e"></div>
            <div class="win-resizer win-resizer-w" data-dir="w"></div>
            <div class="win-resizer win-resizer-ne" data-dir="ne"></div>
            <div class="win-resizer win-resizer-nw" data-dir="nw"></div>
            <div class="win-resizer win-resizer-se" data-dir="se"></div>
            <div class="win-resizer win-resizer-sw" data-dir="sw"></div>
        `;

        document.body.appendChild(winEl);

        const winObj = {
            id,
            el: winEl,
            iframe: winEl.querySelector('iframe'),
            overlay: winEl.querySelector('.win-glass-overlay'),
            isSticky: false,
            isDiminished: false,
            type: type,
            lastHeight: height,
            lastWidth: width,
            lastLeft: left,
            lastTop: top
        };

        this.windows.push(winObj);
        this.setupWindowEvents(winObj);
        this.focusWindow(winObj);

        return winObj;
    }

    setupWindowEvents(win) {
        const header = win.el.querySelector('.window-header');
        
        // Focus on click
        win.el.addEventListener('mousedown', () => this.focusWindow(win), true);

        // Dragging
        header.addEventListener('mousedown', (e) => {
            if (e.target.closest('.win-btn')) return;
            
            this.initialLeft = parseInt(win.el.style.left);
            this.initialTop = parseInt(win.el.style.top);

            this.draggedWindow = win;
            this.initialX = e.clientX;
            this.initialY = e.clientY;
            this.showOverlays();
            document.body.classList.add('win-dragging');
        });

        // Resizing
        const resizers = win.el.querySelectorAll('.win-resizer');
        resizers.forEach(r => {
            r.addEventListener('mousedown', (e) => {
                if (win.isDiminished) return;
                
                e.stopPropagation();
                this.resizedWindow = win;
                this.resizeDir = r.dataset.dir;
                this.initialX = e.clientX;
                this.initialY = e.clientY;
                this.initialWidth = parseInt(win.el.style.width);
                this.initialHeight = parseInt(win.el.style.height);
                this.initialLeft = parseInt(win.el.style.left);
                this.initialTop = parseInt(win.el.style.top);
                this.showOverlays();
                document.body.classList.add('win-resizing');
            });
        });

        // Controls
        win.el.querySelector('.win-close').onclick = () => this.closeWindow(win);
        win.el.querySelector('.win-popout').onclick = () => this.popout(win);
        win.el.querySelector('.win-back').onclick = () => {
            try { win.iframe.contentWindow.history.back(); } catch(e) { console.warn("Navigation failed", e); }
        };
        win.el.querySelector('.win-forward').onclick = () => {
            try { win.iframe.contentWindow.history.forward(); } catch(e) { console.warn("Navigation failed", e); }
        };
        win.el.querySelector('.win-sticky').onclick = () => this.toggleSticky(win);
        win.el.querySelector('.win-diminish').onclick = () => this.toggleDiminish(win);
        win.el.querySelector('.win-dock-left').onclick = () => this.snapTo(win, 'left');
        win.el.querySelector('.win-dock-right').onclick = () => this.snapTo(win, 'right');
        win.el.querySelector('.win-dock-bottom').onclick = () => this.snapTo(win, 'bottom');
    }

    snapTo(win, side) {
        if (win.isDiminished) return;

        const sidebarW = document.body.classList.contains('sidebar-collapsed') ? 70 : 240;
        const trayH = document.getElementById('window-tray').offsetHeight || 40;
        const screenW = window.innerWidth;
        const screenH = window.innerHeight;

        let left, top, width, height;

        if (side === 'left') {
            left = sidebarW;
            top = 0;
            width = (screenW - sidebarW) / 2;
            height = screenH - trayH;
        } else if (side === 'right') {
            left = sidebarW + (screenW - sidebarW) / 2;
            top = 0;
            width = (screenW - sidebarW) / 2;
            height = screenH - trayH;
        } else if (side === 'bottom') {
            left = sidebarW;
            top = screenH / 2;
            width = screenW - sidebarW;
            height = (screenH / 2) - trayH;
        }

        win.el.style.left = left + 'px';
        win.el.style.top = top + 'px';
        win.el.style.width = width + 'px';
        win.el.style.height = height + 'px';
        
        this.focusWindow(win);
    }

    initGlobalEvents() {
        window.addEventListener('mousemove', (e) => {
            if (this.draggedWindow) {
                const dx = e.clientX - this.initialX;
                const dy = e.clientY - this.initialY;
                this.draggedWindow.el.style.left = (this.initialLeft + dx) + 'px';
                this.draggedWindow.el.style.top = (this.initialTop + dy) + 'px';
            } else if (this.resizedWindow) {
                const dx = e.clientX - this.initialX;
                const dy = e.clientY - this.initialY;
                const win = this.resizedWindow.el;
                const dir = this.resizeDir;

                if (dir.includes('e')) {
                    win.style.width = Math.max(200, this.initialWidth + dx) + 'px';
                }
                if (dir.includes('w')) {
                    const newWidth = Math.max(200, this.initialWidth - dx);
                    if (newWidth > 200) {
                        win.style.width = newWidth + 'px';
                        win.style.left = (this.initialLeft + dx) + 'px';
                    }
                }
                if (dir.includes('s')) {
                    win.style.height = Math.max(150, this.initialHeight + dy) + 'px';
                }
                if (dir.includes('n')) {
                    const newHeight = Math.max(150, this.initialHeight - dy);
                    if (newHeight > 150) {
                        win.style.height = newHeight + 'px';
                        win.style.top = (this.initialTop + dy) + 'px';
                    }
                }
            }
        });

        window.addEventListener('mouseup', () => {
            if (this.draggedWindow || this.resizedWindow) {
                this.hideOverlays();
                document.body.classList.remove('win-dragging', 'win-resizing');
            }
            this.draggedWindow = null;
            this.resizedWindow = null;
            this.resizeDir = null;
        });
    }

    focusWindow(win) {
        if (this.activeWindow === win) return;
        this.activeWindow = win;
        
        this.zIndexBase += 10;
        
        win.el.style.zIndex = this.zIndexBase;
        
        this.windows.forEach(w => {
            w.el.classList.remove('active');
        });
        win.el.classList.add('active');
    }

    toggleSticky(win) {
        win.isSticky = !win.isSticky;
        win.el.classList.toggle('sticky', win.isSticky);
        const btn = win.el.querySelector('.win-sticky');
        btn.classList.toggle('active', win.isSticky);
        this.focusWindow(win);
    }

    toggleDiminish(win) {
        win.isDiminished = !win.isDiminished;
        win.el.classList.toggle('diminished', win.isDiminished);
        
        const tray = document.getElementById('window-tray');

        if (win.isDiminished) {
            // Save current state
            win.lastHeight = parseInt(win.el.style.height);
            win.lastWidth = parseInt(win.el.style.width);
            win.lastLeft = parseInt(win.el.style.left);
            win.lastTop = parseInt(win.el.style.top);
            
            // Move to tray
            tray.appendChild(win.el);
            win.el.querySelector('.win-diminish i').className = 'fa-solid fa-window-maximize';
        } else {
            // Move back to body
            document.body.appendChild(win.el);
            
            // Restore state
            win.el.style.height = win.lastHeight + 'px';
            win.el.style.width = win.lastWidth + 'px';
            win.el.style.left = win.lastLeft + 'px';
            win.el.style.top = win.lastTop + 'px';
            win.el.querySelector('.win-diminish i').className = 'fa-solid fa-window-minimize';
            
            this.focusWindow(win);
        }
    }

    updateDiminishedLayout() {
        // Handled by CSS flexbox in #window-tray
    }

    closeWindow(win) {
        win.el.remove();
        this.windows = this.windows.filter(w => w !== win);
        if (this.activeWindow === win) {
            this.activeWindow = this.windows[this.windows.length - 1] || null;
            if (this.activeWindow) this.activeWindow.el.classList.add('active');
        }
        if (win.isDiminished) this.updateDiminishedLayout();
    }

    showOverlays() {
        this.windows.forEach(w => w.overlay.style.display = 'block');
    }

    hideOverlays() {
        this.windows.forEach(w => w.overlay.style.display = 'none');
    }

    popout(win) {
        try {
            const currentUrl = win.iframe.contentWindow.location.href;
            window.open(currentUrl, '_blank');
        } catch (e) {
            // Fallback to initial src if cross-origin or other error
            window.open(win.iframe.src, '_blank');
        }
        this.closeWindow(win);
    }
}
