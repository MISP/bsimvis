/**
 * TableSelection - Excel-like selection for HTML tables
 *
 * Every instance listens on document/window, so a view that mounts several
 * tables (the file view mounts four) has several instances hearing the same
 * event. Two pieces of state are therefore shared rather than per-instance:
 *
 *   TableSelection.active  - the table the user last interacted with. Keyboard
 *                            handling belongs to it alone, otherwise one Enter
 *                            activates a row in every visible table at once.
 *   TableSelection.swallow - the "a synthetic click already handled this
 *                            mouseup" flag. Per-instance it was a race: one
 *                            instance could eat the click another had armed.
 */
class TableSelection {
    constructor(tableId) {
        this.table = document.getElementById(tableId);
        if (!this.table) return;
        if (this.table.tableSelectionInstance) {
            return this.table.tableSelectionInstance;
        }
        // A re-render that replaces the table element leaves the old instance
        // listening forever; retire any instance that lost its element.
        TableSelection.reapDetached();
        this.table.tableSelectionInstance = this;

        this.tbody = this.table.querySelector('tbody');
        this.selectedCells = new Set(); // Stores "row:col"
        this.selectedIds = new Set(); // Stores data-id from tr
        this.selectedEntities = new Set(); // Stores "<etype>\0<eid>" found inside selected cells
        this.focusCell = null; // Current focused cell
        this.focusKey = null; // data-id of the focused row, to refind it after a re-render
        this.anchorCell = null; // Start of range selection
        this.isDragging = false;
        this.cellModeActive = false; // Whether we are in grid-selection mode
        this.startCell = null; // Where the drag started
        this.startPos = { x: 0, y: 0 };
        this.startedOnBlocking = false;
        this.tempFocus = null;

        if (!window.tableSelections) window.tableSelections = [];
        window.tableSelections.push(this);

        this.init();
    }

    /** Pointer travel, in px, still counted as a click rather than a drag. */
    static get CLICK_SLOP() { return 3; }

    /** Shared "swallow the next click" flag -- see the class comment. */
    static get swallow() { return TableSelection._swallow === true; }
    static set swallow(v) { TableSelection._swallow = v === true; }

    /** Arm the swallow, then disarm it once the click that follows has passed. */
    static armSwallow() {
        TableSelection.swallow = true;
        clearTimeout(TableSelection._swallowTimer);
        TableSelection._swallowTimer = setTimeout(() => { TableSelection.swallow = false; }, 50);
    }

    /** Retire instances whose table is no longer in the document. */
    static reapDetached() {
        (window.tableSelections || []).slice().forEach(ts => {
            if (ts.table && !ts.table.isConnected) ts.destroy();
        });
    }

    init() {
        // Kept as fields so destroy() can remove exactly these listeners.
        this._onMouseDown = (e) => this.handleMouseDown(e);
        this._onDragStart = (e) => { if (this.isDragging) e.preventDefault(); };
        this._onClickCapture = (e) => {
            if (TableSelection.swallow) {
                e.preventDefault();
                e.stopPropagation();
                TableSelection.swallow = false;
            }
        };
        this._onMouseMove = (e) => this.handleMouseMove(e);
        this._onMouseUp = (e) => this.handleMouseUp(e);
        this._onKeyDown = (e) => this.handleKeyDown(e);

        document.addEventListener('mousedown', this._onMouseDown);
        document.addEventListener('dragstart', this._onDragStart);
        document.addEventListener('click', this._onClickCapture, true);
        window.addEventListener('mousemove', this._onMouseMove);
        window.addEventListener('mouseup', this._onMouseUp);
        window.addEventListener('keydown', this._onKeyDown);

        // Observer to handle dynamic content
        this.observer = new MutationObserver(() => {
            this.refreshAfterRender();
        });
        this.observer.observe(this.tbody, { childList: true });
    }

    destroy() {
        document.removeEventListener('mousedown', this._onMouseDown);
        document.removeEventListener('dragstart', this._onDragStart);
        document.removeEventListener('click', this._onClickCapture, true);
        window.removeEventListener('mousemove', this._onMouseMove);
        window.removeEventListener('mouseup', this._onMouseUp);
        window.removeEventListener('keydown', this._onKeyDown);
        if (this.observer) this.observer.disconnect();
        if (this.table) this.table.tableSelectionInstance = null;
        if (TableSelection.active === this) TableSelection.active = null;
        const list = window.tableSelections || [];
        const i = list.indexOf(this);
        if (i !== -1) list.splice(i, 1);
    }

    /** Is this table live and on screen? */
    isVisible() {
        return !!(this.tbody && this.tbody.isConnected && this.table.offsetParent);
    }

    /**
     * Which table the keyboard drives: the one last clicked, or -- so arrows
     * still work on a freshly loaded page without clicking first -- the first
     * visible one when nothing has been clicked yet.
     */
    isKeyboardOwner() {
        const active = TableSelection.active;
        if (active && active.isVisible()) return active === this;
        return (window.tableSelections || []).find(ts => ts.isVisible()) === this;
    }

    /** True when the event happened inside this instance's own table. */
    owns(e) {
        const el = (e.target && e.target.nodeType === 3) ? e.target.parentElement : e.target;
        return !!(this.table && el && this.table.contains(el));
    }

    /** Rows that span the grid hold one wide cell; the widest row sets the width. */
    colCount() {
        let n = 0;
        for (const tr of this.tbody.children) n = Math.max(n, tr.children.length);
        return n;
    }

    /**
     * The cell a column index lands on. A row that spans the grid (a group header,
     * a "load more" row) has a single wide cell, so an index past its end resolves
     * to that cell rather than to nothing — which keeps the focused column
     * remembered while arrowing through such a row, the way a spreadsheet does.
     */
    cellAt(r, c) {
        const row = this.tbody.children[r];
        if (!row || !row.children.length) return null;
        return row.children[Math.min(c, row.children.length - 1)];
    }

    /**
     * What a click or Enter on a cell should trigger: the link or handler inside
     * the cell nearest the pointer, else the row's own handler — group header rows
     * carry their expand/collapse on the `<tr>` itself.
     */
    activationTarget(r, c, e) {
        const tr = this.tbody.children[r];
        if (!tr) return null;
        // Controls that act on a row rather than open it. Clicking a cell's dead
        // space should never reach one: the tag editor puts its bookmark button
        // first in the DOM, so before this list a click on the blank part of a
        // Tags cell -- or Enter on it -- silently bookmarked the row.
        const SECONDARY = [
            'remove-tag-btn', 'btn-action', 'btn-copy', 'btn',
            'bookmark-btn', 'ignore-btn', 'add-tag-btn', 'tag-overflow-chip',
            'lineage-toggle', 'btn-note-action', 'btn-icon',
        ];
        const not = SECONDARY.map(cls => `:not(.${cls})`).join('');
        const selector = `a[href]${not}, [onclick]${not}`;
        const cell = this.cellAt(r, c);
        const inCell = cell ? Array.from(cell.querySelectorAll(selector)) : [];
        if (inCell.length) {
            // A pair row stacks both sides in one cell, so the first link in the
            // DOM is only right half the time. Activate the one the click landed
            // next to. Keyboard activation has no pointer, so it takes the first.
            if (!e || inCell.length === 1) return inCell[0];
            // Distance from the pointer to the element's box, zero when inside
            // it. Measuring clientY alone made every control on one flex line
            // tie, and the tie went to DOM order -- horizontal position, the
            // only thing separating them, was ignored.
            const dist = el => {
                const b = el.getBoundingClientRect();
                const dx = Math.max(b.left - e.clientX, 0, e.clientX - b.right);
                const dy = Math.max(b.top - e.clientY, 0, e.clientY - b.bottom);
                return Math.hypot(dx, dy);
            };
            return inCell.reduce((best, el) => (dist(el) < dist(best) ? el : best));
        }
        // Only the row's own action stands in for an empty cell. Reaching into
        // the row for any control at all would fire whatever it happens to hold
        // -- a Delete button, say -- from a click on unrelated whitespace.
        return tr.getAttribute('onclick') ? tr : null;
    }

    /**
     * What identifies a row across a re-render. `data-id` is an entity the row
     * stands for and feeds bulk actions; `data-rowkey` is identity only, for rows
     * that are not an entity at all (a group header).
     */
    rowKey(tr) {
        return (tr && (tr.dataset.rowkey || tr.dataset.id)) || null;
    }

    /**
     * A re-render replaces every row. Keyboard work — expand a group, then keep
     * arrowing from where you were — only survives that if the focused row can be
     * found again, so rows carrying a stable `data-id` keep their focus and
     * anything else clears as before.
     */
    refreshAfterRender() {
        const key = this.focusKey;
        if (!key) {
            this.clearSelection();
            return;
        }
        const r = Array.from(this.tbody.children).findIndex(tr => this.rowKey(tr) === key);
        if (r < 0) {
            this.clearSelection();
            return;
        }
        const c = this.focusCell ? this.focusCell.c : 0;
        this.anchorCell = { r, c };
        this.focusCell = { r, c };
        this.setSelection(r, c, r, c);
        this.updateVisuals();
    }

    getCellInfo(target) {
        // Handle text nodes or other non-element targets
        const el = (target && target.nodeType === 3) ? target.parentElement : target;
        if (!el || typeof el.closest !== 'function') return null;

        const td = el.closest('td');
        if (!td) return null;
        const tr = td.parentElement;
        if (!tr || tr.parentElement !== this.tbody) return null;

        const rowIndex = Array.from(this.tbody.children).indexOf(tr);
        const colIndex = Array.from(tr.children).indexOf(td);

        return { r: rowIndex, c: colIndex, element: td };
    }

    isInteractive(target) {
        let el = (target && target.nodeType === 3) ? target.parentElement : target;
        if (!el || el.nodeType !== 1) return false;

        const interactiveTags = ['INPUT', 'SELECT', 'BUTTON', 'A', 'TEXTAREA'];
        if (interactiveTags.includes(el.tagName)) return true;
        if (el.closest('button') || el.closest('a')) return true;

        // Check for specific clickable classes or attributes
        if (el.onclick || el.getAttribute('onclick')) return true;
        if (el.classList.contains('btn-copy') || el.classList.contains('btn-action')) return true;

        return false;
    }

    handleMouseDown(e) {
        if (e.button !== 0) return;

        // Clear selection if clicking outside the table container
        const container = this.table.closest('.table-container') || this.table.parentElement;
        if (!container.contains(e.target) && !e.target.closest('.context-menu') && !e.target.closest('#graph-context-menu') && !this.isInteractive(e.target)) {
            this.clearSelection();
        }

        // Exclude interactive elements, sidebar, floating windows, code panes, or other graph/visual containers
        if (this.isInteractive(e.target)) return;
        if (
            e.target.closest('nav') ||
            e.target.closest('#window-tray') ||
            e.target.closest('.code-preview-scroll') ||
            e.target.closest('.diff-pane') ||
            e.target.closest('.c-code-container') ||
            e.target.closest('#graph-view-container') ||
            e.target.closest('#hierarchy-view-container') ||
            e.target.closest('#packing-view-container') ||
            e.target.closest('#call-graph-view-container')
        ) {
            return;
        }

        // Only the table under the pointer starts a drag. Without this every
        // instance on the page armed its own drag/swallow state from one
        // mousedown, so a click in one table could be eaten by another.
        if (!this.owns(e)) return;
        TableSelection.active = this;

        const info = this.getCellInfo(e.target);

        this.isDragging = true;
        this.startPos = { x: e.clientX, y: e.clientY };
        this.startCell = info ? { r: info.r, c: info.c } : null;
        this.cellModeActive = false;
        
        // We track if we started on a truly "blocking" element like a button or actual <a> link.
        // Plain divs/spans with pointer cursor (like filenames) should still allow drag-to-cell-select.
        this.startedOnBlocking = this.isInteractive(e.target);

        if (info && e.shiftKey && this.anchorCell) {
            this.cellModeActive = true;
            this.extendSelection(info.r, info.c);
            this.updateVisuals();
            e.preventDefault();
        } else if (info) {
            this.tempFocus = { r: info.r, c: info.c };
        }
    }

    handleMouseMove(e) {
        if (!this.isDragging) return;

        const targetEl = document.elementFromPoint(e.clientX, e.clientY) || e.target;
        const info = this.getCellInfo(targetEl);
        if (!info) return;

        if (!this.startCell) {
            this.startCell = { r: info.r, c: info.c };
        }

        const movedSignificantly = Math.hypot(e.clientX - this.startPos.x, e.clientY - this.startPos.y) > 5;

        // Activate cell mode if we move to a different cell OR move significantly from a non-blocking start
        if (!this.cellModeActive && (info.r !== this.startCell.r || info.c !== this.startCell.c)) {
            this.cellModeActive = true;
            this.anchorCell = { r: this.startCell.r, c: this.startCell.c };
            
            window.getSelection().removeAllRanges();
            
            this.setSelection(this.anchorCell.r, this.anchorCell.c, info.r, info.c);
            this.focusCell = { r: info.r, c: info.c };
            this.updateVisuals();
        } 
        
        if (this.cellModeActive) {
            e.preventDefault(); 
            if (info.r !== this.focusCell.r || info.c !== this.focusCell.c) {
                this.extendSelection(info.r, info.c);
                this.updateVisuals();
            }
        }
    }

    handleMouseUp(e) {
        if (this.isDragging) {
            const dist = Math.hypot(e.clientX - this.startPos.x, e.clientY - this.startPos.y);
            const selection = window.getSelection().toString();

            // One threshold decides both questions. They used to disagree --
            // the swallow armed above 3px while activation ran below 3px -- so a
            // 4px shaky click armed the swallow, then activated, and had its own
            // synthetic click eaten: a click that did nothing at all.
            const isClick = dist <= TableSelection.CLICK_SLOP;

            if (this.cellModeActive || !isClick) {
                TableSelection.armSwallow();
            }

            if (!this.cellModeActive && this.tempFocus && !this.startedOnBlocking) {
                // A click with nothing selected focuses the cell and follows its
                // link. Text selected inside the cell means the user was
                // selecting, not navigating -- leave it alone.
                if (isClick && !selection) {
                    this.clearSelection();
                    this.anchorCell = { r: this.tempFocus.r, c: this.tempFocus.c };
                    this.focusCell = { r: this.tempFocus.r, c: this.tempFocus.c };
                    this.setSelection(this.tempFocus.r, this.tempFocus.c, this.tempFocus.r, this.tempFocus.c);
                    this.updateVisuals();

                    const link = this.activationTarget(this.tempFocus.r, this.tempFocus.c, e);
                    if (link && e.target !== link && !link.contains(e.target)) {
                        link.click();
                        // The native click still follows this mouseup and would
                        // reach the row's own onclick -- one click, two
                        // navigations. Swallow it the same way a drag does.
                        TableSelection.armSwallow();
                    }
                }
            }
        }
        this.isDragging = false;
        this.tempFocus = null;
        this.startedOnBlocking = false;
    }

    handleKeyDown(e) {
        // Bail out if this instance's table is no longer in the live DOM (stale SPA
        // instance), or is on a panel/tab that is currently hidden — arrow keys go
        // to the table you can see, without having to click it first.
        if (!this.tbody || !this.tbody.isConnected || !this.table.offsetParent) {
            return;
        }

        // Escape clears every table, focused or not.
        if (e.key === 'Escape') {
            this.clearSelection();
            return;
        }

        // Everything below moves or activates a cell, so only one table may act.
        // Without this, Enter on a view with four tables fired four navigations.
        if (!this.isKeyboardOwner()) {
            return;
        }

        if (document.activeElement.isContentEditable || document.activeElement.tagName === 'TEXTAREA') {
            return;
        }

        if (document.activeElement.tagName === 'INPUT') {
            const input = document.activeElement;
            // Alt+Arrow is reserved for navbar shortcuts — don't intercept it here
            if (!e.altKey && (e.key === 'ArrowUp' || e.key === 'ArrowDown')) {
                input.blur();
                e.preventDefault();
                // Continue to table selection
            } else if (!e.altKey && (e.key === 'ArrowLeft' || e.key === 'ArrowRight') && input.value === '') {
                input.blur();
                e.preventDefault();
                // Continue to table selection
            } else {
                return;
            }
        }

        if (e.key === 'a' && (e.ctrlKey || e.metaKey)) {
            e.preventDefault();
            this.selectAll();
            return;
        }

        if (e.key === 'c' && (e.ctrlKey || e.metaKey)) {
            const hasTextSelection = window.getSelection() && window.getSelection().toString().length > 0;
            if (this.selectedCells.size > 0 && !hasTextSelection) {
                e.preventDefault();
                this.copySelection();
                return;
            }
        }

        if ((e.ctrlKey || e.metaKey || e.altKey) && (e.key === 'ArrowLeft' || e.key === 'ArrowRight')) {
            return;
        }

        if (e.key === 'Enter') {
            if (this.focusCell) {
                e.preventDefault();
                const target = this.activationTarget(this.focusCell.r, this.focusCell.c);
                if (target) target.click();
            }
            return;
        }

        const rows = this.tbody.children.length;
        if (rows === 0) return;
        const cols = this.colCount();

        if (!this.focusCell) {
            if (['ArrowUp', 'ArrowDown', 'ArrowLeft', 'ArrowRight'].includes(e.key)) {
                e.preventDefault();
                this.anchorCell = { r: 0, c: 0 };
                this.focusCell = { r: 0, c: 0 };
                this.setSelection(0, 0, 0, 0);
                this.updateVisuals();
                this.scrollIntoView();
            }
            return;
        }

        const { r, c } = this.focusCell;
        let newR = r;
        let newC = c;

        if (e.key === 'ArrowUp') {
            newR = Math.max(0, r - 1);
        } else if (e.key === 'ArrowDown') {
            newR = Math.min(rows - 1, r + 1);
        } else if (e.key === 'ArrowLeft') {
            newC = Math.max(0, c - 1);
        } else if (e.key === 'ArrowRight') {
            newC = Math.min(cols - 1, c + 1);
        } else {
            return;
        }

        e.preventDefault();

        if (e.ctrlKey || e.metaKey) {
            // Move to edge
            if (e.key === 'ArrowUp') newR = 0;
            if (e.key === 'ArrowDown') newR = rows - 1;
            if (e.key === 'ArrowLeft') newC = 0;
            if (e.key === 'ArrowRight') newC = cols - 1;
        }

        if (e.shiftKey) {
            this.extendSelection(newR, newC);
        } else {
            this.anchorCell = { r: newR, c: newC };
            this.focusCell = { r: newR, c: newC };
            this.setSelection(newR, newC, newR, newC);
        }

        this.updateVisuals();
        this.scrollIntoView();
    }

    setSelection(r1, c1, r2, c2) {
        this.selectedCells.clear();
        this.selectedIds.clear();
        this.selectedEntities.clear();
        const startR = Math.min(r1, r2);
        const endR = Math.max(r1, r2);
        const startC = Math.min(c1, c2);
        const endC = Math.max(c1, c2);

        for (let r = startR; r <= endR; r++) {
            const row = this.tbody.children[r];
            if (row && row.dataset.id) {
                this.selectedIds.add(row.dataset.id);
            }
            for (let c = startC; c <= endC; c++) {
                this.selectedCells.add(`${r}:${c}`);
                // Rows that carry no data-id (bin-diff matched rows) — or that hold
                // several entities side by side (function A / function B / the pair)
                // — are resolved from the tag editors inside the selected cells.
                const cell = this.cellAt(r, c);
                if (!cell) continue;
                cell.querySelectorAll('[data-etype][data-eid]').forEach(el => {
                    this.selectedEntities.add(`${el.dataset.etype} ${el.dataset.eid}`);
                });
            }
        }
    }

    selectAll() {
        const rows = this.tbody.children.length;
        if (rows === 0) return;
        const cols = this.colCount();

        this.anchorCell = { r: 0, c: 0 };
        this.focusCell = { r: rows - 1, c: cols - 1 };
        this.setSelection(0, 0, rows - 1, cols - 1);
        this.updateVisuals();
    }

    getSelectedIds() {
        return Array.from(this.selectedIds);
    }

    /** [{ etype, eid }] for every tag editor inside the selected cells. */
    getSelectedEntities() {
        return Array.from(this.selectedEntities).map(k => {
            const i = k.indexOf('\0');
            return { etype: k.slice(0, i), eid: k.slice(i + 1) };
        });
    }

    extendSelection(r, c) {
        this.focusCell = { r, c };
        this.setSelection(this.anchorCell.r, this.anchorCell.c, r, c);
    }

    clearSelection() {
        this.selectedCells.clear();
        this.selectedIds.clear();
        this.selectedEntities.clear();
        this.focusCell = null;
        this.anchorCell = null;
        this.updateVisuals();
    }

    updateVisuals() {
        // Clear old classes
        this.tbody.querySelectorAll('.selected-cell, .focused-cell, .selected-row, .sel-t, .sel-b, .sel-l, .sel-r').forEach(el => {
            el.classList.remove('selected-cell', 'focused-cell', 'selected-row', 'sel-t', 'sel-b', 'sel-l', 'sel-r');
        });

        if (this.selectedCells.size === 0) {
            return;
        }

        let minR = Infinity, maxR = -Infinity, minC = Infinity, maxC = -Infinity;

        this.selectedCells.forEach(coord => {
            const [r, c] = coord.split(':').map(Number);
            if (r < minR) minR = r;
            if (r > maxR) maxR = r;
            if (c < minC) minC = c;
            if (c > maxC) maxC = c;

            const cell = this.cellAt(r, c);
            if (cell) {
                cell.classList.add('selected-cell');
            }
        });

        // Apply edge classes to boundary cells
        for (let r = minR; r <= maxR; r++) {
            const row = this.tbody.children[r];
            if (!row) continue;
            for (let c = minC; c <= maxC; c++) {
                const cell = this.cellAt(r, c);
                if (!cell) continue;
                if (r === minR) cell.classList.add('sel-t');
                if (r === maxR) cell.classList.add('sel-b');
                if (c === minC) cell.classList.add('sel-l');
                if (c === maxC) cell.classList.add('sel-r');
            }
        }

        const focusRow = this.focusCell ? this.tbody.children[this.focusCell.r] : null;
        if (focusRow) {
            const focusCell = this.cellAt(this.focusCell.r, this.focusCell.c);
            if (focusCell) {
                focusCell.classList.add('focused-cell');
            }
        }
        // Remembered here rather than at every move: this runs after each focus
        // change, and it is the last chance to read the row before a re-render
        // replaces it. See refreshAfterRender.
        this.focusKey = this.rowKey(focusRow);
    }

    scrollIntoView() {
        if (!this.focusCell) return;
        const cell = this.cellAt(this.focusCell.r, this.focusCell.c);
        if (!cell) return;

        const rect = cell.getBoundingClientRect();
        // The table's own scroll box. Found by walking up rather than by id: the
        // main table's wrap is always in the DOM, so any other table that named it
        // would scroll the wrong view.
        let container = this.table.parentElement;
        for (let el = container; el && el !== document.body; el = el.parentElement) {
            const oy = getComputedStyle(el).overflowY;
            if ((oy === 'auto' || oy === 'scroll') && el.scrollHeight > el.clientHeight) {
                container = el;
                break;
            }
        }
        if (!container) return;
        const containerRect = container.getBoundingClientRect();

        if (rect.top < containerRect.top) {
            container.scrollTop -= (containerRect.top - rect.top + 40);
        } else if (rect.bottom > containerRect.bottom) {
            container.scrollTop += (rect.bottom - containerRect.bottom + 40);
        }
    }

    async copySelection() {
        if (this.selectedCells.size === 0) return;

        const isSimilarityTable = window.location.pathname.includes('/function-similarity');

        // Helper to clean and extract text from an element, formatting tag editors nicely
        const cleanAndGetText = (el) => {
            if (!el) return '';
            const clone = el.cloneNode(true);

            // Handle cluster cells specifically to export only comma-separated UUIDs
            const clusterCells = clone.querySelectorAll('.cluster-cards-cell');
            const allClusterCells = [...clusterCells];
            if (clone.classList && clone.classList.contains('cluster-cards-cell')) {
                allClusterCells.push(clone);
            }
            allClusterCells.forEach(cell => {
                const dataStr = cell.getAttribute('data-clusters');
                if (dataStr) {
                    try {
                        const clusters = JSON.parse(dataStr);
                        const threshold = typeof UIParams !== 'undefined' ? UIParams.cohesionThreshold : 0.5;
                        const validClusters = clusters.filter(c => (c.cohesion_score || 0) >= threshold);
                        const uuids = validClusters.map(c => c.cluster_uuid).filter(Boolean);
                        const textNode = document.createTextNode(uuids.join(', '));
                        if (cell.parentNode) {
                            cell.parentNode.replaceChild(textNode, cell);
                        } else {
                            cell.innerHTML = '';
                            cell.appendChild(textNode);
                        }
                    } catch (err) {
                        console.error('Failed to parse clusters JSON', err);
                    }
                }
            });

            const tagEditors = clone.querySelectorAll('.entity-tags-editor, .sim-tags-editor');
            tagEditors.forEach(tagEditor => {
                const tags = [];
                if (tagEditor.querySelector('.bookmark-btn.active')) {
                    tags.push('bookmark');
                }
                if (tagEditor.querySelector('.ignore-btn.active')) {
                    tags.push('ignore');
                }
                tagEditor.querySelectorAll('.analysis-tag-badge').forEach(e => {
                    tags.push(e.textContent.trim());
                });
                tagEditor.querySelectorAll('.sim-tag-card').forEach(e => {
                    const removeBtn = e.querySelector('.remove-tag-btn');
                    if (removeBtn) removeBtn.remove();
                    tags.push(e.textContent.trim());
                });
                const textNode = document.createTextNode(tags.join(', '));
                tagEditor.parentNode.replaceChild(textNode, tagEditor);
            });
            clone.querySelectorAll('button, .btn-copy, .btn-action, script, style').forEach(e => e.remove());
            let text = clone.innerText.trim();
            // Remove leading @ and # (along with any space following them)
            text = text.replace(/^@\s*/, '').replace(/^#\s*/, '');
            return text;
        };

        const getSimilarityScore = (cellEl) => {
            const divs = cellEl.querySelectorAll('div');
            for (let div of divs) {
                if (div.textContent.includes('%')) {
                    const match = div.textContent.match(/\d+(\.\d+)?%/);
                    if (match) return match[0];
                }
            }
            return '';
        };

        const getSimilarityTags = (cellEl) => {
            const tagEditor = cellEl.querySelector('.sim-tags-editor');
            if (!tagEditor) return '';
            const tags = [];
            if (tagEditor.querySelector('.bookmark-btn.active')) {
                tags.push('bookmark');
            }
            if (tagEditor.querySelector('.ignore-btn.active')) {
                tags.push('ignore');
            }
            tagEditor.querySelectorAll('.analysis-tag-badge').forEach(el => {
                tags.push(el.textContent.trim());
            });
            tagEditor.querySelectorAll('.sim-tag-card').forEach(el => {
                const removeBtn = el.querySelector('.remove-tag-btn');
                if (removeBtn) removeBtn.remove();
                tags.push(el.textContent.trim());
            });
            return tags.join(', ');
        };

        const getStackedElements = (cellEl) => {
            const wrapper = cellEl.firstElementChild;
            if (wrapper && wrapper.children.length === 2) {
                return [wrapper.children[0], wrapper.children[1]];
            }
            return [cellEl, cellEl];
        };

        // Sort selected cells by row then col
        const coords = Array.from(this.selectedCells).map(s => s.split(':').map(Number));
        coords.sort((a, b) => a[0] - b[0] || a[1] - b[1]);

        // Group selected columns by row
        const rowColsMap = {};
        coords.forEach(([r, c]) => {
            if (!rowColsMap[r]) rowColsMap[r] = [];
            rowColsMap[r].push(c);
        });

        const finalRows = [];

        Object.keys(rowColsMap).map(Number).sort((a, b) => a - b).forEach(r => {
            const rowEl = this.tbody.children[r];
            if (!rowEl) return;

            const cols = rowColsMap[r];

            if (isSimilarityTable) {
                // Similarity table: Map one row to two output rows (one for each function)
                const line1 = [];
                const line2 = [];

                cols.forEach(c => {
                    const cellEl = rowEl.children[c];
                    if (!cellEl) {
                        line1.push('');
                        line2.push('');
                        return;
                    }

                    if (c === 0) {
                        // Similarity column: Split into Score and Tags (only fill on first line)
                        const score = getSimilarityScore(cellEl);
                        const tags = getSimilarityTags(cellEl);
                        line1.push(score, tags);
                        line2.push('', '');
                    } else {
                        // All other columns are stacked: line1 gets function 1, line2 gets function 2
                        const [f1El, f2El] = getStackedElements(cellEl);
                        line1.push(cleanAndGetText(f1El));
                        line2.push(cleanAndGetText(f2El));
                    }
                });

                finalRows.push(line1);
                finalRows.push(line2);
            } else {
                // Standard table: Map one row to one output row
                const line = [];
                cols.forEach(c => {
                    const cellEl = rowEl.children[c];
                    if (window.location.pathname.includes('/files') && c === 1) {
                        // Split MD5 and Arch/Language
                        if (cellEl) {
                            const divs = cellEl.querySelectorAll('div');
                            if (divs.length === 2) {
                                line.push(cleanAndGetText(divs[0]), cleanAndGetText(divs[1]));
                            } else {
                                line.push(cleanAndGetText(cellEl), '');
                            }
                        } else {
                            line.push('', '');
                        }
                    } else if (window.location.pathname.includes('/clusters') && c === 0) {
                        // Split UUID and ID in cluster view
                        if (cellEl) {
                            const uuid = cellEl.getAttribute('data-uuid') || cleanAndGetText(cellEl).split('ID:')[0].trim();
                            const id = cellEl.getAttribute('data-id') || (cellEl.querySelector('div')?.textContent || '').replace('ID:', '').trim();
                            line.push(uuid, id);
                        } else {
                            line.push('', '');
                        }
                    } else {
                        line.push(cleanAndGetText(cellEl));
                    }
                });
                finalRows.push(line);
            }
        });

        if (finalRows.length === 0) return;

        // Build header line if enabled
        const uiParams = window.UIParams || (typeof UIParams !== 'undefined' ? UIParams : null);
        const includeHeaders = uiParams && uiParams.includeHeaders;
        let headerLine = null;
        if (includeHeaders && Object.keys(rowColsMap).length > 0) {
            headerLine = [];
            const thead = this.table.querySelector('thead');
            const headerRow = thead ? thead.querySelector('tr') : null;
            const firstRowIndex = Object.keys(rowColsMap).map(Number).sort((a, b) => a - b)[0];
            const cols = rowColsMap[firstRowIndex] || [];

            cols.forEach(c => {
                const thEl = headerRow ? headerRow.children[c] : null;
                let label = thEl ? (thEl.getAttribute('data-label') || thEl.textContent.trim()) : `Col ${c}`;
                label = label.replace(/[▼▲↕]/g, '').trim();

                if (isSimilarityTable) {
                    if (c === 0) {
                        headerLine.push('Similarity Score', 'Similarity Tags');
                    } else {
                        headerLine.push(label);
                    }
                } else {
                    if (window.location.pathname.includes('/files') && c === 1) {
                        headerLine.push('MD5', 'Arch');
                    } else if (window.location.pathname.includes('/clusters') && c === 0) {
                        headerLine.push('UUID', 'ID');
                    } else {
                        headerLine.push(label);
                    }
                }
            });
        }

        // CSV for Ctrl+Shift+V (Plain text copy)
        let csvRows = [...finalRows];
        if (headerLine) {
            csvRows.unshift(headerLine);
        }
        const csvContent = csvRows
            .map(row => row.map(cell => `"${cell.replace(/"/g, '""')}"`).join(','))
            .join('\n');

        // HTML Table for Ctrl+V in Excel/Calc (Easy paste)
        // Adding basic styling to help Excel recognize it as a table
        let htmlContent = '<table border="1" style="border-collapse: collapse;">';
        if (headerLine) {
            htmlContent += '<tr style="font-weight: bold; background-color: #f2f2f2;">';
            headerLine.forEach(cell => {
                const escaped = cell.replace(/&/g, '&amp;')
                                   .replace(/</g, '&lt;')
                                   .replace(/>/g, '&gt;')
                                   .replace(/"/g, '&quot;');
                htmlContent += `<th style="border: 1px solid var(--meta-text-muted); padding: 4px; text-align: left;">${escaped}</th>`;
            });
            htmlContent += '</tr>';
        }
        finalRows.forEach(row => {
            htmlContent += '<tr>';
            row.forEach(cell => {
                const escaped = cell.replace(/&/g, '&amp;')
                                   .replace(/</g, '&lt;')
                                   .replace(/>/g, '&gt;')
                                   .replace(/"/g, '&quot;');
                htmlContent += `<td style="border: 1px solid var(--meta-text-muted); padding: 4px;">${escaped}</td>`;
            });
            htmlContent += '</tr>';
        });
        htmlContent += '</table>';

        let success = false;
        if (typeof ClipboardItem !== 'undefined' && navigator.clipboard && typeof navigator.clipboard.write === 'function') {
            try {
                const data = [new ClipboardItem({
                    'text/plain': new Blob([csvContent], { type: 'text/plain' }),
                    'text/html': new Blob([htmlContent], { type: 'text/html' })
                })];
                await navigator.clipboard.write(data);
                success = true;
                console.log('Selection copied to clipboard (HTML & CSV)');
            } catch (err) {
                console.warn('ClipboardItem write failed, trying fallback...', err);
            }
        }

        if (!success) {
            // Fallback 1: Try document.execCommand('copy') with custom copy listener for rich HTML table + CSV
            try {
                const listener = function(e) {
                    e.clipboardData.setData('text/html', htmlContent);
                    e.clipboardData.setData('text/plain', csvContent);
                    e.preventDefault();
                };
                document.addEventListener('copy', listener);
                success = document.execCommand('copy');
                document.removeEventListener('copy', listener);
                if (success) {
                    console.log('Selection copied to clipboard via fallback HTML/CSV');
                }
            } catch (err) {
                console.warn("Fallback rich copy failed, trying plain text copy...", err);
            }
        }

        if (!success) {
            // Fallback 2: Try document.execCommand('copy') with a temporary textarea (CSV text only)
            try {
                const textArea = document.createElement("textarea");
                textArea.value = csvContent;
                textArea.style.position = "fixed";
                textArea.style.top = "0";
                textArea.style.left = "0";
                textArea.style.opacity = "0";
                document.body.appendChild(textArea);
                textArea.focus();
                textArea.select();
                success = document.execCommand('copy');
                document.body.removeChild(textArea);
                if (success) {
                    console.log('Selection copied to clipboard via fallback CSV only');
                }
            } catch (err) {
                console.error("All table copy methods failed", err);
            }
        }
    }
}

// `class` declarations live in script scope, not on window — export explicitly
window.TableSelection = TableSelection;

/**
 * Ids of the current table selection. With `etype` given, only entities of that
 * kind come back — so selecting both function columns of a bin diff yields the
 * two functions, and selecting the similarity column yields the pairs.
 */
window.getSelectedTableIds = (etype = null) => {
    const allIds = new Set();
    if (window.tableSelections) {
        window.tableSelections.forEach(ts => {
            ts.getSelectedEntities().forEach(({ etype: t, eid }) => {
                if (!etype || t === etype) allIds.add(eid);
            });
            ts.getSelectedIds().forEach(id => {
                // Row ids carry no type of their own; keep one only when the page
                // actually renders it as an entity of the requested kind.
                if (etype && !document.querySelector(`[data-etype="${etype}"][data-eid="${CSS.escape(id)}"]`)) return;
                allIds.add(id);
            });
        });
    }
    return Array.from(allIds);
};

// Initialize when DOM is ready
document.addEventListener('DOMContentLoaded', () => {
    // Target the body table specifically (header table has no selectable data)
    const bodyTable = document.getElementById('data-table');
    if (bodyTable) {
        new TableSelection('data-table');
        return;
    }

    // Fallback: find tables in containers (for other pages like feature tables)
    const containers = document.querySelectorAll('.table-container, .feature-table-container');
    if (containers.length > 0) {
        containers.forEach(container => {
            const table = container.querySelector('tbody') && container.querySelector('table');
            if (table) {
                if (!table.id) table.id = 'table-' + Math.random().toString(36).substr(2, 9);
                new TableSelection(table.id);
            }
        });
    } else {
        const tables = document.querySelectorAll('table');
        tables.forEach(table => {
            if (!table.id) table.id = 'table-' + Math.random().toString(36).substr(2, 9);
            new TableSelection(table.id);
        });
    }
});
