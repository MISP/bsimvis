/**
 * Compare basket: files collected across pages/searches for one N-way diff.
 *
 * Kept in sessionStorage (shared by the app's same-origin iframes); every access
 * is wrapped so the page still works, per window, when storage is blocked.
 */
(function () {
    const KEY = 'bsim_nway_basket';
    let fallback = [];

    function read() {
        try {
            return JSON.parse(sessionStorage.getItem(KEY)) || [];
        } catch (e) {
            return fallback;
        }
    }

    function write(items) {
        fallback = items;
        try {
            sessionStorage.setItem(KEY, JSON.stringify(items));
        } catch (e) {}
    }

    // A file id may be `coll:file:md5` or `coll:md5`; the basket stores `coll:md5`.
    function norm(id, collection) {
        const parts = String(id).split(':');
        return `${parts[0] && parts.length > 1 ? parts[0] : collection || ''}:${parts.pop().toLowerCase()}`;
    }

    function render() {
        // Only the top window owns the chip; iframes ask it to redraw.
        try {
            if (window.top !== window && window.top.NwayBasket) return window.top.NwayBasket.render();
        } catch (e) {}
        const n = read().length;
        document.querySelectorAll('#nway-basket-status').forEach(el => {
            // Same footer pill as the other status chips; hover lists the files.
            const names = read().map(t => `#${t.split(':').pop().slice(0, 8)} (${t.slice(0, t.lastIndexOf(':'))})`).join('\n');
            el.innerHTML = n
                ? `<span class="table-footer-badge nway-basket" title="${escapeAttr(names)}">
                    <i class="fa-solid fa-table-columns"></i> ${n} in compare set
                    ${n >= 2 ? '<button class="nway-basket-open" onclick="NwayBasket.open(event)">Open N-way &#8599;</button>' : ''}
                    <button onclick="NwayBasket.clear()" title="Clear compare set">&times;</button>
                </span>`
                : '';
        });
    }

    window.NwayBasket = {
        items: () => read().slice(),
        add(ids, collection) {
            const have = new Set(read());
            for (const id of ids) have.add(norm(id, collection));
            write([...have]);
            render();
        },
        clear() {
            write([]);
            render();
        },
        render,
        open(event) {
            const items = read();
            if (items.length < 2) return;
            const url = NwayPanel.urlFor(items, '');
            Nav.openPath(url, event, { title: 'N-way diff', type: 'bin_sim' });
        },
    };

    document.addEventListener('DOMContentLoaded', render);
})();
