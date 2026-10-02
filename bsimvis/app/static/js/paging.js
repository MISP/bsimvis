/**
 * Shared list paging. The mode is a per-user UI setting (UIParams.pagingMode):
 *   'page'   Prev/Next footer; the view keeps `offset` in its URL so a page can be shared.
 *   'scroll' rows append as the sentinel nears the viewport; `offset` stays out of the URL
 *            and an `offset` in an opened link is ignored.
 * Views own their fetching; this only supplies the mode, the footer and the sentinel.
 */
window.Paging = {
    mode() {
        return window.UIParams && window.UIParams.pagingMode === 'scroll' ? 'scroll' : 'page';
    },

    // `shown` is how many rows are loaded so far (scroll mode counts from the top).
    footer({ offset, total, size, shown }) {
        const scroll = this.mode() === 'scroll';
        const from = total ? (scroll ? 1 : offset + 1) : 0;
        const to = scroll ? shown : Math.min(offset + size, total);
        const badge = `<span class="table-footer-badge">${from}&ndash;${to} of ${total}</span>`;
        if (scroll) return badge;
        const [prev, next] = this.buttons({ offset, total, size });
        return prev + badge + next;
    },

    // [Prev, Next] for views that show the count badge elsewhere.
    buttons({ offset, total, size }) {
        return [
            `<button class="top-action-btn" data-page="-1"${offset <= 0 ? ' disabled' : ''}>Prev</button>`,
            `<button class="top-action-btn" data-page="1"${offset + size >= total ? ' disabled' : ''}>Next</button>`,
        ];
    },

    // Calls `onNear` whenever the sentinel is (or comes) within reach. Observing
    // again after each append re-fires it if the sentinel is still in view.
    observe(sentinel, onNear) {
        const io = new IntersectionObserver(es => { if (es.some(e => e.isIntersecting)) onNear(); }, { rootMargin: '300px' });
        io.observe(sentinel);
        return io;
    },
};
