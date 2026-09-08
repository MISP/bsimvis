// Shared function-filter UI used by list, detail, similarity and saved-search views.
window.FunctionFilters = {
    commonFields: [
        { param: 'q', label: 'Any text' },
        { param: 'function_name', label: 'Function name' },
        { param: 'namespace', label: 'Namespace' },
        { param: 'return_type', label: 'Return type' },
        { param: 'entrypoint_address', label: 'Address' },
        { param: 'func_tag', label: 'Function tags', multiple: true },
        { param: 'exclude_func_tag', label: 'Exclude function tags', multiple: true },
        { param: 'cluster_name', label: 'Cluster name' },
        { param: 'cluster_uuid', label: 'Cluster UUID' },
        { param: 'min_cohesion', label: 'Min cohesion', type: 'number' },
        { param: 'min_features', label: 'Min features', type: 'number' },
        { param: 'note_owner', label: 'Note owner' },
        { param: 'file_name', label: 'File name' },
        { param: 'file_md5', label: 'File MD5' },
        { param: 'file_tag', label: 'File tags', multiple: true },
        { param: 'exclude_file_tag', label: 'Exclude file tags', multiple: true },
        { param: 'language_id', label: 'Language' },
    ],

    readIdMap(idMap) {
        const out = {};
        for (const [id, param] of Object.entries(idMap)) {
            const value = (document.getElementById(id)?.value || '').trim();
            if (value) out[param] = value;
        }
        return out;
    },

    setParams(params, idMap) {
        for (const [key, value] of Object.entries(this.readIdMap(idMap))) params.set(key, value);
        return params;
    },

    cell(id, { type = 'text', placeholder = '', value = '', onInput, onKeydown, attrs = '', style = 'width:100%; box-sizing:border-box;' } = {}) {
        const esc = value => typeof escapeAttr === 'function' ? escapeAttr(String(value)) : String(value);
        const handler = onInput ? ` oninput="${esc(onInput)}"` : '';
        const keyHandler = onKeydown ? ` onkeydown="${esc(onKeydown)}"` : '';
        return `<input type="${esc(type)}" id="${esc(id)}" placeholder="${esc(placeholder)}" value="${esc(value)}" style="${esc(style)}" ${attrs}${handler}${keyHandler} />`;
    },

    rangeCell(idMin, idMax, { onInput, onKeydown, valueMin = '', valueMax = '', step = '0.05', min = '0', max = '1' } = {}) {
        const attrs = [`step="${step}"`, min === '' ? '' : `min="${min}"`, max === '' ? '' : `max="${max}"`].filter(Boolean).join(' ');
        const options = { type: 'number', attrs, onInput, onKeydown, style: 'font-size:0.65rem; width:48%; box-sizing:border-box;' };
        return `<div style="display:flex; align-items:center; gap:2px;">`
            + this.cell(idMin, { ...options, placeholder: 'Min...', value: valueMin })
            + `<span class="dim" style="font-size:0.6rem">-</span>`
            + this.cell(idMax, { ...options, placeholder: 'Max...', value: valueMax })
            + `</div>`;
    },

    filterRow(cells) {
        return `<tr class="filter-row">${cells.map(cell => `<th${cell.attrs ? ` ${cell.attrs}` : ''}>${cell.html || ''}</th>`).join('')}</tr>`;
    },

    functionRow({ values = {}, onInput, onKeydown, onFocus = () => '', tagCell = '', leadingCells = [], extraCells = [] } = {}) {
        const input = (id, param, placeholder, options = {}) => this.cell(id, {
            placeholder,
            value: values[param] || '',
            onInput,
            onKeydown,
            attrs: [onFocus(param), options.attrs || ''].filter(Boolean).join(' '),
            type: options.type || 'text',
            style: options.style || 'width:100%; box-sizing:border-box; font-size:0.65rem;',
        });
        const cells = [
            ...leadingCells,
            { html: `<div style="display:flex; flex-direction:column; gap:4px;">${input('flt-func-name', 'function_name', 'Name...')}<div style="display:flex; gap:2px;">${input('flt-func-namespace', 'namespace', 'Namespace...', { style: 'width:50%; box-sizing:border-box; font-size:0.6rem;' })}${input('flt-func-ret_type', 'return_type', 'Return type...', { style: 'width:50%; box-sizing:border-box; font-size:0.6rem;' })}</div></div>` },
            { html: input('flt-func-address', 'entrypoint_address', 'Addr...') },
            { html: tagCell, attrs: 'style="position:relative"' },
            { html: `<div style="display:flex; flex-direction:column; gap:2px;">${input('flt-func-cluster', 'cluster_uuid', 'UUID...', { style: 'width:100%; box-sizing:border-box; font-size:0.6rem;' })}${input('flt-func-cluster-name', 'cluster_name', 'Cluster name...', { style: 'width:100%; box-sizing:border-box; font-size:0.6rem;' })}${input('flt-func-min-cohesion', 'min_cohesion', 'Min cohesion...', { type: 'number', attrs: 'step="0.05" min="0" max="1"', style: 'width:100%; box-sizing:border-box; font-size:0.6rem;' })}</div>` },
            { html: input('flt-func-min-features', 'min_features', 'Min', { type: 'number', attrs: 'min="0"' }) },
            { html: input('flt-func-note-owner', 'note_owner', 'Note owner...') },
            ...extraCells,
        ];
        return this.filterRow(cells);
    },

    searchForm(prefix) {
        const fields = this.commonFields.map(field => `
            <label style="display:flex; flex-direction:column; gap:4px; font-size:0.72rem; color:var(--dim);">${field.label}
                ${this.cell(`${prefix}-${field.param}`, {
                    type: field.type || 'text',
                    placeholder: field.label,
                    attrs: `data-param="${field.param}"${field.multiple ? ' data-multiple="true"' : ''}`,
                    style: 'padding:7px; width:100%; box-sizing:border-box;',
                })}
            </label>`).join('');
        return `<div style="display:grid; grid-template-columns:repeat(auto-fit,minmax(150px,1fr)); gap:8px;">${fields}</div>
            <label style="display:flex; flex-direction:column; gap:4px; margin-top:8px; font-size:0.72rem; color:var(--dim);">Additional API filters
                ${this.cell(`${prefix}-raw`, { placeholder: 'e.g. static_tag=libc&sort_by=features', style: 'padding:7px; width:100%; box-sizing:border-box;' })}
            </label>
            <div class="dim" style="font-size:0.68rem; margin-top:6px;">Separate multiple tags with commas. Additional filters use /api/function/search query syntax.</div>`;
    },

    searchQuery(prefix) {
        const raw = document.getElementById(`${prefix}-raw`)?.value.trim() || '';
        const params = new URLSearchParams(raw);
        document.querySelectorAll(`[id^="${prefix}-"][data-param]`).forEach(input => {
            const value = input.value.trim();
            if (!value) return;
            if (input.dataset.multiple === 'true') {
                value.split(',').map(v => v.trim()).filter(Boolean).forEach(v => params.append(input.dataset.param, v));
            } else {
                params.set(input.dataset.param, value);
            }
        });
        return params.toString();
    },
};
