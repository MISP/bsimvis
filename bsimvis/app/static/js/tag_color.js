// A tag's colour, derived from its id.
//
// The rule and its parameters live in `tag_taxonomy.py`; this is the mirror
// that applies them in the browser, because the UI colours ids the backend
// never sees -- folding `origin:lib:libc:2.31` up to `origin:lib:libc` happens
// here, and that folded node still needs its colour. `/api/tags/colors` ships
// the parameters so only the arithmetic is duplicated, and
// `scripts/test_tag_colors.js` pins both sides to the same vectors.
//
// Hue comes from subdividing the colour wheel by the segments of the id, so a
// family shares an arc; tone separates siblings inside that arc; step lightens
// anything deeper than the tag's own level, so a leaf reads as a shade of its
// group.
const TagColor = (() => {
    let cfg = null;

    // FNV-1a over UTF-16 code units, matching `_hash32` in tag_taxonomy.py.
    // `Math.imul` keeps the multiply 32-bit; `>>> 0` keeps it unsigned.
    function hash32(text) {
        let h = 0x811c9dc5;
        for (let i = 0; i < text.length; i++) {
            h = Math.imul(h ^ (text.charCodeAt(i) & 0xffff), 0x01000193) >>> 0;
        }
        return h;
    }

    // `{hue, tone, step}`; hue null means "nothing to hash", drawn grey.
    function style(tagId) {
        const id = String(tagId || '').includes(':') ? String(tagId) : `user:${tagId}`;
        if (!cfg) return { hue: null, tone: 0, step: 0 };
        const ns = id.split(':')[0];
        const sep = new RegExp(cfg.hue_split[ns] || cfg.hue_split_default);
        const segs = id.split(sep).filter(Boolean);
        const rest = segs.slice(1);
        if (!rest.length) return { hue: null, tone: 0, step: 0 };

        const depth = cfg.hue_depth[ns] ?? cfg.hue_depth_default;
        const step = Math.min(Math.max(0, rest.length - depth), cfg.max_step);
        if (ns === 'severity' && cfg.severity_hues[rest[0]] !== undefined) {
            return { hue: cfg.severity_hues[rest[0]], tone: 1, step };
        }

        let lo = 0, span = 360, h = 0;
        for (let i = 0; i < Math.min(depth, rest.length); i++) {
            h = hash32(segs.slice(0, i + 2).join(':'));
            const width = span * cfg.hue_shrink[Math.min(i, cfg.hue_shrink.length - 1)];
            lo += (h % cfg.hue_slots) * (span - width) / (cfg.hue_slots - 1);
            span = width;
        }
        return {
            hue: Math.round((lo + span / 2) * 100) / 100,
            tone: (h >>> 20) % cfg.tones,
            step,
        };
    }

    // The colour to paint. Saturation and lightness come from CSS variables, so
    // a hue is the only thing fixed per tag and both themes stay legible.
    // `gray` drops the hue entirely -- unmatched mass has no tag agreement to
    // report, and a grey that still shades by depth keeps stacked bands apart
    // without competing with the coloured flows.
    function css(tagId, opts) {
        const { hue, tone, step } = style(tagId);
        const lum = `calc(var(--tagc-l${tone}) + ${step * (cfg ? cfg.step_lum : 0)}%)`;
        if (hue === null || (opts && opts.gray)) {
            return `hsl(0, 0%, ${lum})`;
        }
        return `hsl(${hue}, var(--tagc-s${tone}), ${lum})`;
    }

    // A tag a human coloured by hand keeps that colour: it is a deliberate mark
    // on one collection's vocabulary, which is exactly what a derived colour
    // cannot know about.
    function forTag(tagId, opts) {
        const meta = (window.tagMetadata || {})[tagId];
        if (meta && meta.color && !(opts && opts.gray)) return meta.color;
        return css(tagId, opts);
    }

    const ready = fetch('/api/tags/colors')
        .then(r => r.json())
        .then(c => { cfg = c; return c; })
        .catch(e => { console.error('tag colour config failed to load', e); });

    return { style, css, forTag, ready, hash32, config: () => cfg };
})();

window.TagColor = TagColor;
