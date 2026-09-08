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
    // `(namespace, levels, detail)`, mirroring `tag_levels` in tag_taxonomy.py.
    // The detail tail is dropped before splitting, so a function or rule name
    // can never become a level -- the same cut the index makes when it builds
    // ancestor buckets.
    function levels(tagId) {
        const { body, ns, pattern } = parts(tagId);
        return { ns, segs: body.split(new RegExp(pattern)).filter(Boolean) };
    }

    // Split shape shared by `levels` and `prefixes`. Falls back to a plain colon
    // before `/api/tags/colors` has answered, so a tree drawn early nests
    // correctly and only its colours arrive late.
    function parts(tagId) {
        const detail = (cfg && cfg.tag_detail) || '#';
        const body = String(tagId).split(detail)[0];
        const ns = body.split(':')[0];
        const seps = (cfg && (cfg.tag_separators[ns] || cfg.tag_separators_default)) || [':'];
        const pattern = seps
            .slice()
            .sort((a, b) => b.length - a.length)
            .map(s => s.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'))
            .join('|');
        return { body, ns, pattern };
    }

    // Ancestor prefixes of a tag id, excluding itself -- the mirror of
    // `tag_prefixes` in tag_taxonomy.py, and the same values the search index
    // buckets under. `lib:libc:2.31#memcpy` stops at `lib:libc`: the detail tail
    // is not a level, so nothing named after a function can become a node.
    function prefixes(tagId) {
        const { body, pattern } = parts(tagId);
        const pieces = body.split(new RegExp('(' + pattern + ')'));
        const out = [];
        let prefix = pieces[0];
        for (let i = 1; i < pieces.length - 1; i += 2) {
            if (prefix) out.push(prefix);
            prefix += pieces[i] + pieces[i + 1];
        }
        return out;
    }

    // The id with its detail tail removed: the deepest node a tag can occupy.
    function groupId(tagId) {
        return parts(tagId).body;
    }

    // The levels a tag id occupies, outermost first, every one a real tag id:
    // `['fid', 'fid:libc', 'fid:libc:2.31']`. The tree in any view is this walk,
    // so a node id is always a string some tag actually produces -- which is
    // what makes its colour, its index buckets and its filter the same string.
    function chain(tagId) {
        const out = prefixes(tagId);
        out.push(groupId(tagId));
        return out;
    }

    // Which axis a tag answers, from the map the backend ships. Kept here rather
    // than copied into each view: a second table is how a tag ends up on one
    // axis in the tree and another in the graph.
    function axisOf(tagId) {
        const ns = parts(tagId).ns;
        if (!cfg) return ns;
        return cfg.tag_axes[ns] || cfg.tag_axis_default;
    }

    function style(tagId) {
        const id = String(tagId || '').includes(':') ? String(tagId) : `user:${tagId}`;
        if (!cfg) return { hue: null, tone: 0, step: 0 };
        const { ns, segs } = levels(id);
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

    return {
        style, css, forTag, ready, hash32,
        levels, prefixes, groupId, chain, axisOf,
        config: () => cfg,
    };
})();

window.TagColor = TagColor;
