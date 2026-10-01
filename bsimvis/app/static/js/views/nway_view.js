/**
 * N-way diff view: /diff/nway?md5s=coll:md5,coll:md5[&pool=P][&tab=..&min_edge=..]
 *
 * Reads the member list and the control state from the URL, mounts NwayPanel,
 * and writes the state back so the page is deep-linkable.
 */
window.NwayView = {
    _panel: null,

    destroy() {
        if (this._panel) this._panel.destroy();
        this._panel = null;
    },

    async init(params, containerId) {
        const container = document.getElementById(containerId);
        if (!container) return;
        const q = new URLSearchParams(window.location.search);
        if (!q.get('md5s')) {
            container.innerHTML = '<div class="dim" style="padding:20px;">Pick two or more files in a file table and use "Compare N files".</div>';
            return;
        }
        container.innerHTML = '<div style="padding:16px; overflow:auto; width:100%;"><h3 style="margin:0 0 10px;">N-way file diff</h3><div id="nway-panel"></div></div>';
        const source = { md5s: q.get('md5s') };
        for (const k of ['pool', 'collection']) if (q.get(k)) source[k] = q.get(k);
        const state = {};
        for (const k of ['tab', 'scope', 'k', 'min_edge', 'mode', 'q', 'tags', 'sort_col', 'sort_dir', 'offset']) {
            if (q.has(k)) state[k] = q.get(k);
        }
        this._panel = new NwayPanel(document.getElementById('nway-panel'), {
            source,
            state,
            onState: s => {
                const next = new URLSearchParams({ md5s: source.md5s });
                for (const k of ['pool', 'collection']) if (source[k]) next.set(k, source[k]);
                for (const [k, v] of Object.entries(s)) if (v !== '' && v !== null && v !== undefined) next.set(k, v);
                history.replaceState(null, '', `${window.location.pathname}?${next.toString()}`);
            },
        });
        this._panel.load();
    },
};
