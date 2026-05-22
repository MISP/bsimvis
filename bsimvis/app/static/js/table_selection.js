/**
 * TableSelection - Excel-like selection for HTML tables
 */
class TableSelection {
    constructor(tableId) {
        this.table = document.getElementById(tableId);
        if (!this.table) return;

        this.tbody = this.table.querySelector('tbody');
        this.selectedCells = new Set(); // Stores "row:col"
        this.selectedIds = new Set(); // Stores data-id from tr
        this.focusCell = null; // Current focused cell
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

    init() {
        this.table.addEventListener('mousedown', (e) => this.handleMouseDown(e));
        this.table.addEventListener('dragstart', (e) => {
            if (this.isDragging) {
                e.preventDefault();
            }
        });
        window.addEventListener('mousemove', (e) => this.handleMouseMove(e));
        window.addEventListener('mouseup', (e) => this.handleMouseUp(e));
        window.addEventListener('keydown', (e) => this.handleKeyDown(e));

        document.addEventListener('mousedown', (e) => {
            if (!this.table.contains(e.target)) {
                this.clearSelection();
            }
        });

        // Observer to handle dynamic content
        this.observer = new MutationObserver(() => {
            this.clearSelection();
        });
        this.observer.observe(this.tbody, { childList: true });
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

        const info = this.getCellInfo(e.target);
        if (!info) return;

        this.isDragging = true;
        this.startPos = { x: e.clientX, y: e.clientY };
        this.startCell = { r: info.r, c: info.c };
        this.cellModeActive = false;
        
        // We track if we started on a truly "blocking" element like a button or actual <a> link.
        // Plain divs/spans with pointer cursor (like filenames) should still allow drag-to-cell-select.
        this.startedOnBlocking = this.isInteractive(e.target);

        if (e.shiftKey && this.anchorCell) {
            this.cellModeActive = true;
            this.extendSelection(info.r, info.c);
            this.updateVisuals();
            e.preventDefault();
        } else {
            this.tempFocus = { r: info.r, c: info.c };
        }
    }

    handleMouseMove(e) {
        if (!this.isDragging) return;

        const info = this.getCellInfo(e.target);
        if (!info) return;

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

            if (!this.cellModeActive && this.tempFocus && !this.startedOnBlocking) {
                // If it was just a click or a very small movement with no text selection,
                // we treat it as focusing the cell.
                if (!selection || dist < 3) {
                    this.clearSelection();
                    this.anchorCell = { r: this.tempFocus.r, c: this.tempFocus.c };
                    this.focusCell = { r: this.tempFocus.r, c: this.tempFocus.c };
                    this.setSelection(this.tempFocus.r, this.tempFocus.c, this.tempFocus.r, this.tempFocus.c);
                    this.updateVisuals();
                }
            }
        }
        this.isDragging = false;
        this.tempFocus = null;
        this.startedOnBlocking = false;
    }

    handleKeyDown(e) {
        if (e.key === 'Escape') {
            this.clearSelection();
            return;
        }

        // Only handle if no input is focused, OR if the table is the intended target
        if (document.activeElement.tagName === 'INPUT' || document.activeElement.tagName === 'TEXTAREA') {
            // Allow Ctrl+C even if input is focused? No, usually not.
            return;
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

        if (!this.focusCell) return;

        const { r, c } = this.focusCell;
        let newR = r;
        let newC = c;

        const rows = this.tbody.children.length;
        if (rows === 0) return;
        const cols = this.tbody.children[0].children.length;

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
            }
        }
    }

    selectAll() {
        const rows = this.tbody.children.length;
        if (rows === 0) return;
        const cols = this.tbody.children[0].children.length;

        this.anchorCell = { r: 0, c: 0 };
        this.focusCell = { r: rows - 1, c: cols - 1 };
        this.setSelection(0, 0, rows - 1, cols - 1);
        this.updateVisuals();
    }

    getSelectedIds() {
        return Array.from(this.selectedIds);
    }

    extendSelection(r, c) {
        this.focusCell = { r, c };
        this.setSelection(this.anchorCell.r, this.anchorCell.c, r, c);
    }

    clearSelection() {
        this.selectedCells.clear();
        this.selectedIds.clear();
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
            
            const row = this.tbody.children[r];
            if (row) {
                const cell = row.children[c];
                if (cell) {
                    cell.classList.add('selected-cell');
                }
            }
        });

        // Apply edge classes to boundary cells
        for (let r = minR; r <= maxR; r++) {
            const row = this.tbody.children[r];
            if (!row) continue;
            for (let c = minC; c <= maxC; c++) {
                const cell = row.children[c];
                if (!cell) continue;
                if (r === minR) cell.classList.add('sel-t');
                if (r === maxR) cell.classList.add('sel-b');
                if (c === minC) cell.classList.add('sel-l');
                if (c === maxC) cell.classList.add('sel-r');
            }
        }

        const focusRow = this.focusCell ? this.tbody.children[this.focusCell.r] : null;
        if (focusRow) {
            const focusCell = focusRow.children[this.focusCell.c];
            if (focusCell) {
                focusCell.classList.add('focused-cell');
            }
        }
    }

    scrollIntoView() {
        if (!this.focusCell) return;
        const row = this.tbody.children[this.focusCell.r];
        if (!row) return;
        const cell = row.children[this.focusCell.c];
        if (!cell) return;

        const rect = cell.getBoundingClientRect();
        // Scroll the body wrap div (parent of the body table)
        const container = document.getElementById('table-body-wrap') || this.table.parentElement;
        const containerRect = container.getBoundingClientRect();

        if (rect.top < containerRect.top) {
            container.scrollTop -= (containerRect.top - rect.top + 40);
        } else if (rect.bottom > containerRect.bottom) {
            container.scrollTop += (rect.bottom - containerRect.bottom + 40);
        }
    }

    copySelection() {
        if (this.selectedCells.size === 0) return;

        const isSimilarityTable = window.location.hash.startsWith('#function-similarity');

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
                    if (window.location.hash.startsWith('#files') && c === 1) {
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
                    } else if (window.location.hash.startsWith('#clusters') && c === 0) {
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
            const thead = document.getElementById('table-head');
            const headerRow = thead ? thead.querySelector('tr') : null;
            const firstRowIndex = Object.keys(rowColsMap).map(Number).sort((a, b) => a - b)[0];
            const cols = rowColsMap[firstRowIndex] || [];

            cols.forEach(c => {
                const thEl = headerRow ? headerRow.children[c] : null;
                const label = thEl ? (thEl.getAttribute('data-label') || thEl.textContent.trim()) : `Col ${c}`;

                if (isSimilarityTable) {
                    if (c === 0) {
                        headerLine.push('Similarity Score', 'Similarity Tags');
                    } else {
                        headerLine.push(label);
                    }
                } else {
                    if (window.location.hash.startsWith('#files') && c === 1) {
                        headerLine.push('MD5', 'Arch');
                    } else if (window.location.hash.startsWith('#clusters') && c === 0) {
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
                htmlContent += `<th style="border: 1px solid #ccc; padding: 4px; text-align: left;">${escaped}</th>`;
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
                htmlContent += `<td style="border: 1px solid #ccc; padding: 4px;">${escaped}</td>`;
            });
            htmlContent += '</tr>';
        });
        htmlContent += '</table>';

        if (navigator.clipboard && window.ClipboardItem) {
            try {
                const data = [new ClipboardItem({
                    'text/plain': new Blob([csvContent], { type: 'text/plain' }),
                    'text/html': new Blob([htmlContent], { type: 'text/html' })
                })];
                navigator.clipboard.write(data).then(() => {
                    console.log('Selection copied to clipboard (HTML & CSV)');
                }).catch(err => {
                    console.error('ClipboardItem write failed', err);
                    navigator.clipboard.writeText(csvContent);
                });
            } catch (err) {
                console.error('ClipboardItem construction failed', err);
                navigator.clipboard.writeText(csvContent);
            }
        } else {
            navigator.clipboard.writeText(csvContent);
        }
    }
}

window.getSelectedTableIds = () => {
    const allIds = new Set();
    if (window.tableSelections) {
        window.tableSelections.forEach(ts => {
            ts.getSelectedIds().forEach(id => allIds.add(id));
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
