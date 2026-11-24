/**
 * Shared list paging. The mode is a per-user UI setting (UIParams.pagingMode):
 *   'page'   "‹ Page [n] / N ›" footer; the view keeps `offset` in its URL so a page can be shared.
 *   'scroll' rows append as the sentinel nears the viewport; `offset` stays out of the URL
 *            and an `offset` in an opened link is ignored.
 * Views own their fetching; this only supplies the mode, the footer and the sentinel.
 */
window.Paging = {
    mode() {
        return window.UIParams && window.UIParams.pagingMode === 'scroll' ? 'scroll' : 'page';
    },

    // `‹ Page [n] / N ›`, for views that show their own row count elsewhere.
    controls({ offset, total, size }) {
        const pages = Math.max(1, Math.ceil(total / size));
        const page = Math.min(pages, Math.floor(offset / size) + 1);
        return `<button class="top-action-btn paging-btn" data-page="-1" title="Previous page"${page <= 1 ? ' disabled' : ''}>&lsaquo;</button>`
            + `<span class="paging-page">Page <input class="paging-input" type="number" min="1" max="${pages}" value="${page}" data-page-input title="Go to page"> / ${pages.toLocaleString()}</span>`
            + `<button class="top-action-btn paging-btn" data-page="1" title="Next page"${page >= pages ? ' disabled' : ''}>&rsaquo;</button>`;
    },

    // `shown` is how many rows are loaded so far (scroll mode counts from the top).
    footer({ offset, total, size, shown }) {
        if (this.mode() === 'scroll') return `<span class="table-footer-badge">${total ? 1 : 0}&ndash;${shown} of ${total.toLocaleString()}</span>`;
        return `${this.controls({ offset, total, size })}<span class="table-footer-badge">${total.toLocaleString()} rows</span>`;
    },

    // Wires the arrows and the page box inside `el` (once; the markup can be
    // re-rendered under it). `state()` -> {offset, total, size}; `go(offset)` loads that page.
    bind(el, state, go) {
        if (!el || el._pagingBound) return;
        el._pagingBound = true;
        const to = d => {
            const { offset, total, size } = state();
            const last = Math.max(0, Math.ceil(total / size) - 1) * size;
            return Math.min(last, Math.max(0, d.page !== undefined ? (d.page - 1) * size : offset + d.delta * size));
        };
        el.addEventListener('click', e => {
            const b = e.target.closest('[data-page]');
            if (b && !b.disabled) go(to({ delta: Number(b.dataset.page) }));
        });
        el.addEventListener('change', e => {
            if (!e.target.matches('[data-page-input]')) return;
            const p = parseInt(e.target.value, 10);
            if (p >= 1) go(to({ page: p }));
        });
    },

    // Calls `onNear` whenever the sentinel is (or comes) within reach. Observing
    // again after each append re-fires it if the sentinel is still in view.
    observe(sentinel, onNear) {
        const io = new IntersectionObserver(es => { if (es.some(e => e.isIntersecting)) onNear(); }, { rootMargin: '300px' });
        io.observe(sentinel);
        return io;
    },
};
