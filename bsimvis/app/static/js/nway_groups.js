/**
 * Group header pieces shared by the N-way table (nway_panel.js) and the lineage page
 * (nway_lineage.js): the lane name the model gives a group, what backs it (prefix hint,
 * capa / yara findings, strings, call graph button), the sub-lane chip, the stats and
 * role mix, the band icon and the track key. A group is the meta /nway and
 * /nway/lineage send (nway_group_service).
 *
 * Naming state S = { names: {id: name}, evid: {id: {tags, strings, calls}}, busy, done, off }
 * is the caller's. `nameLanes` polls POST /api/bin_sim/nway/lane_names and keeps every
 * answer in one cache keyed by the lane's content (what the server hashes), so a lane the
 * table named is named on the lineage page with no second model call.
 * Every value off a sample goes through escapeHtml / escapeAttr.
 */
window.NwayGroups = (function () {
    // Triage roles in reading order (nway_role_service.ROLES).
    const ROLES = [
        ['orchestrator', 'Orchestrator', 'Calls many functions of its file: read first for the program flow.', 'fa-sitemap'],
        ['entry', 'Entry point', 'No caller in the call graph: thread, signal handler or pointer-table entry.', 'fa-door-open'],
        ['logic', 'Logic', 'Calls and is called: the body of each feature.', 'fa-code'],
        ['compute', 'Compute', 'Large and calls nothing: decoders, checksums, crypto.', 'fa-calculator'],
        ['leaf', 'Leaf helper', 'Small and calls nothing.', 'fa-leaf'],
        ['plumbing', 'Plumbing', 'Stubs, one-call wrappers and helpers called from everywhere.', 'fa-plug'],
        ['runtime', 'Runtime', 'C runtime start-up and teardown.', 'fa-gears'],
        ['library', 'Library', 'Recognised by the library analyzer (FunctionID, stdlib reference).', 'fa-book'],
    ];
    // Group header icons by kind and key: tiers, presence bands, deltas, special regions.
    const BAND_ICONS = {
        core: 'fa-bullseye', common: 'fa-circle-half-stroke', rare: 'fa-circle-dot',
        every: 'fa-bullseye', most: 'fa-chart-pie', some: 'fa-circle-half-stroke', few: 'fa-circle-dot',
        added: 'fa-plus', dropped: 'fa-minus', rewritten: 'fa-pen-to-square', span: 'fa-arrows-left-right', scattered: 'fa-shuffle', kept: 'fa-equals',
        small: 'fa-cubes', none: 'fa-link-slash', runtime: 'fa-gears', library: 'fa-book', rest: 'fa-ellipsis',
    };
    const pct = v => `${(Number(v || 0) * 100).toFixed(0)}%`;
    const shownName = name => (!name || /^unnamed/i.test(name) ? '' : name);

    // The groups the model names: lanes, presence / delta bands and unlabelled regions, with something to read.
    const nameable = g => (g.kind === 'lane' || g.kind === 'presence' || g.kind === 'delta' || (g.kind === 'region' && !g.label))
        && !!((g.sample || []).length || g.hint || (g.tags || []).length || (g.fids || []).length);
    // One lane of the naming request; `nFiles` = real file count (a mask bit may be a column group).
    const payload = (g, nFiles, strings) => ({
        id: g.id, files: g.files || g.mask.split('1').length - 1, total: nFiles || g.mask.length, rows: g.rows, sample: g.sample || [], hint: g.hint || '', tags: g.tags || [], fids: g.fids || [], name_fids: g.name_fids || [], tag_rows: g.tag_rows || 0,
        ...(strings ? { kind: 'string' } : g.kind === 'presence' || g.kind === 'delta' ? { kind: 'summary' } : {}),
    });
    const cache = new Map();
    const keyOf = l => { const { id, rows, ...rest } = l; return JSON.stringify(rest); };

    // Names `lanes` (payloads): cached ones at once, the rest by polling while `live()`.
    // onNames(names, evidence) gets every batch. -> 'ready' | 'off' | 'pending' (gave up) | 'stale'.
    async function nameLanes(lanes, live, onNames) {
        const hit = { names: {}, evidence: {} };
        for (const l of lanes) { const c = cache.get(keyOf(l)); if (c) { hit.names[l.id] = c.name; hit.evidence[l.id] = c.evidence; } }
        if (Object.keys(hit.names).length) onNames(hit.names, hit.evidence);
        if (lanes.every(l => cache.has(keyOf(l)))) return 'ready';
        let status = 'pending';
        for (let i = 0; i < 120 && live(); i++) {
            let body = null;
            try {
                const res = await fetch('/api/bin_sim/nway/lane_names', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ lanes }) });
                body = res.ok ? await res.json() : null;
            } catch (e) { body = null; }
            if (!live()) return 'stale';
            if (body && body.names) {
                for (const l of lanes) if (body.names[l.id]) cache.set(keyOf(l), { name: body.names[l.id], evidence: (body.evidence || {})[l.id] });
                onNames(body.names, body.evidence || {});
            }
            status = body ? body.status : 'failed';
            if (status !== 'pending') break;
            await new Promise(r => setTimeout(r, 1000));
        }
        return live() ? status : 'stale';
    }

    // A lane the model could not describe (the call failed): its main library calls, else a plain note.
    function laneFallback(g, S) {
        const calls = (((S.evid || {})[g.id] || {}).calls || []).slice(0, 3);
        return calls.length ? `calls ${calls.join(', ')}` : 'unlabelled';
    }

    // The lane's name once the model has answered; a placeholder while it is still working.
    function laneName(g, S) {
        const name = shownName((S.names || {})[g.id]);
        const wait = S.busy && !name && ((g.sample || []).length || g.hint || (g.tags || []).length || (g.fids || []).length);
        const fb = !name && !wait && !S.off && S.done ? laneFallback(g, S) : '';
        return `<span class="nway-lname${wait ? ' wait' : ''}${S.off ? ' off' : ''}${fb ? ' fb' : ''}" data-lname="${escapeAttr(g.id)}"${fb ? ' title="The model could not describe this lane; this is what it is made of."' : ''}>${escapeHtml(name || fb)}</span>`;
    }

    // The nearest lane above this one: a sub-lane holds only files that are also in it.
    function laneParent(g, groups) {
        if (g.under_id) return groups[g.under_id] || null;
        const path = g.path || [];
        for (let i = path.length - 2; i >= 0; i--) if ((groups[path[i]] || {}).kind === 'lane') return groups[path[i]];
        return null;
    }

    // "⊂ network scanning": which lane this one is a part of, by name once known.
    function parentChip(g, groups, S) {
        const par = laneParent(g, groups);
        const files = par ? par.mask.split('1').length - 1 : Number(g.under) || 0;
        const label = (par && shownName((S.names || {})[par.id])) || (files ? `${files}-file lane` : '');
        if (!label) return '';
        return `<span class="nway-gchip" data-lpar="${escapeAttr(g.id)}" title="${escapeAttr(`Sub-lane of ${label}: its files are all in that lane.`)}">⊂ ${escapeHtml(label)}</span>`;
    }

    // The lane's capa / yara findings (group meta plus what the naming call saw), without the rule-detail tail.
    function laneTags(g, S) {
        const ev = (S.evid || {})[g.id] || {};
        return [...new Set([...(g.tags || []), ...(ev.tags || [])].map(t => t.split('#')[0]))].slice(0, 4);
    }

    // A finding drawn like a tag is everywhere else: its own colour, on a tinted badge.
    function tagBadge(t) {
        const color = window.tagInk(window.getTagMetadata(t).color);
        return `<span class="analysis-tag-badge nway-ltag" style="border-color:${tagAlpha(color, 40)}; color:${color}; background:${tagAlpha(color, 7)};" title="${escapeAttr(t)}: a finding on this lane's functions">${escapeHtml(t)}</span>`;
    }

    // The colour of the lane's first finding: a stripe at the lane's left edge.
    function laneColor(g, S) {
        const t = laneTags(g, S)[0];
        return t ? window.tagInk(window.getTagMetadata(t).color) : '';
    }

    // What backs the name: the shared prefix, findings and a few strings the lane's functions use.
    function laneEv(g, S, graph = true) {
        const ev = (S.evid || {})[g.id] || {};
        const graphBtn = `<button type="button" class="nway-gchip nway-lanegraph" data-lane-graph="${escapeAttr(g.id)}" title="Open the call graph of this lane's functions together: who calls them, what they call, and whether they are called together"><i class="fa-solid fa-diagram-project"></i> call graph</button>`;
        const chip = (text, title, cls) => `<span class="nway-gchip${cls ? ` ${cls}` : ''}" title="${escapeAttr(title)}">${escapeHtml(text)}</span>`;
        return (graph ? graphBtn : '') + (g.hint ? chip(g.hint, "Most of this lane's functions share this name prefix") : '')
            + laneTags(g, S).map(tagBadge).join('')
            + (ev.strings || []).map(t => chip(`"${t}"`, 'A string the lane\'s functions use', 'str')).join('');
    }

    function roleIcon(role) {
        const r = ROLES.find(x => x[0] === role);
        return r ? `<i class="fa-solid ${r[3]} nway-ricon r-${r[0]}"></i>` : '';
    }

    // A header's role mix: one stripe per role, as wide as its share.
    function roleMix(roles) {
        const parts = ROLES.filter(([k]) => roles[k]), n = parts.reduce((a, [k]) => a + roles[k], 0);
        if (!n) return '';
        return `<span class="nway-rmix" title="${escapeAttr(parts.map(([k, l]) => `${l} ${Number(roles[k])}`).join(' · '))}">${parts.map(([k]) => `<i class="r-${k}" style="width:${(100 * roles[k] / n).toFixed(1)}%"></i>`).join('')}</span>`;
    }

    // A group header's icon: its tier, band, delta pattern or special region.
    function bandIcon(g) {
        const last = String(g.id).split('/').pop();
        const key = g.kind === 'region' ? g.region : last.slice(last.indexOf(':') + 1).split('-')[0];
        const icon = BAND_ICONS[key];
        return icon ? `<i class="fa-solid ${icon} nway-bicon"></i>` : '';
    }

    // Support / cohesion use the same red-to-green ramp as the other score cells.
    function score(v) {
        let color = '';
        try { color = typeof window.scoreColor === 'function' ? window.scoreColor(v) : ''; } catch (e) { color = ''; }
        return `<span${color ? ` style="color:${escapeAttr(color)}"` : ''}>${pct(v)}</span>`;
    }

    // What a header shows in the row columns: span and feature ranges, mean support, mean (and lowest) cohesion.
    function stats(g) {
        const range = (a, b) => (a == null ? '' : a === b ? `${Number(a)}` : `${Number(a)}–${Number(b)}`);
        const low = g.cohesion_min != null && g.cohesion != null && g.cohesion_min < g.cohesion - 0.005
            ? `<span class="nway-gmin" title="Lowest cohesion in the group">↓${pct(g.cohesion_min)}</span>` : '';
        return {
            span: range(g.span_lo, g.span_hi),
            feat: g.kind === 'same' ? range(Math.round(g.w_lo), Math.round(g.w_hi)) : Number(g.weight || 0).toFixed(0),
            support: g.support == null ? '' : score(g.support),
            cohesion: g.cohesion == null ? '' : score(g.cohesion) + low,
        };
    }

    // A group's track key, `<lens>:<key>` for the kinds a lens names: region -> modules:<region id>,
    // presence -> core:<band>, role -> triage:<role>, born -> deltas:<column id>, lane -> lanes:<mask hex,
    // bit i = the column's default-order index base(i), as the lineage page's node masks>. Any other kind
    // (tier, delta, tag, prefix, same, fold, rest) keeps its group id.
    function trackKey(g, gid, base) {
        if (g && g.kind === 'born' && g.column) return `deltas:${g.column}`;
        const last = String(gid).split('/').pop(), i = last.indexOf(':'), kind = i > 0 ? last.slice(0, i) : '', key = last.slice(i + 1);
        const lens = { region: 'modules', presence: 'core', role: 'triage', born: 'deltas' }[kind];
        if (lens) return `${lens}:${key}`;
        if (kind === 'lane' && /^[01]+$/.test(key)) return `lanes:${[...key].reduce((m, b, ci) => (b === '1' ? m | (1n << BigInt(base(ci))) : m), 0n).toString(16)}`;
        return String(gid);
    }

    return { ROLES, BAND_ICONS, shownName, nameable, payload, nameLanes, laneFallback, laneName, laneParent, parentChip, laneTags, tagBadge, laneColor, laneEv, roleIcon, roleMix, bandIcon, score, stats, trackKey };
})();
