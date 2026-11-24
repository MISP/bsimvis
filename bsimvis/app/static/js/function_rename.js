// Analyst / LLM function names, shared by every view that lists functions.
// The name lives on the function doc (`user_name`); the LLM only suggests, the analyst accepts.
(function () {
    const post = (url, body) => fetch(url, { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) }).then(async r => ({ ok: r.ok, body: await r.json().catch(() => ({})) }));
    const overlay = inner => {
        const box = document.createElement('div');
        box.style.cssText = 'position:fixed;inset:0;z-index:10000;background:rgba(0,0,0,.45);display:flex;align-items:center;justify-content:center';
        box.innerHTML = inner;
        document.body.appendChild(box);
        return box;
    };
    const done = renames => document.dispatchEvent(new CustomEvent('function-renamed', { detail: { renames } }));

    // One name for the functions in `fids`: manual entry or an LLM suggestion. Empty name resets.
    function open(fids, current) {
        const box = overlay(`<div style="background:var(--bg-card,#1e1e1e);padding:16px;border-radius:8px;min-width:340px;display:flex;flex-direction:column;gap:8px">
            <b>Rename function</b>
            <input type="text" maxlength="64" value="${escapeAttr(current)}" style="padding:6px">
            <div class="dim" data-msg style="min-height:1.2em"></div>
            <div style="display:flex;gap:8px;justify-content:flex-end">
                <button data-ai title="Ask the LLM for a name">AI suggest</button>
                <button data-reset title="Back to the derived name">Reset</button>
                <button data-cancel>Cancel</button>
                <button data-save>Save</button>
            </div></div>`);
        const input = box.querySelector('input'), msg = box.querySelector('[data-msg]');
        let owner = 'user';
        input.focus();
        input.select();
        input.addEventListener('input', () => { owner = 'user'; });
        const close = () => box.remove();
        const save = async name => {
            const { ok, body } = await post('/api/notes/rename', { func_ids: fids, name, owner });
            if (!ok) { msg.textContent = body.error || 'Rename failed'; return; }
            close();
            done([{ fids, name }]);
        };
        box.addEventListener('click', async e => {
            if (e.target === box || e.target.closest('[data-cancel]')) return close();
            if (e.target.closest('[data-save]')) return save(input.value.trim());
            if (e.target.closest('[data-reset]')) return save('');
            if (e.target.closest('[data-ai]')) {
                const ai = box.querySelector('[data-ai]');
                if (ai.disabled) return;
                ai.disabled = input.disabled = true;
                msg.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> Asking the LLM...';
                const { ok, body } = await post('/api/notes/rename/suggest', { func_ids: fids });
                ai.disabled = input.disabled = false;
                if (!ok) { msg.textContent = body.error || 'No suggestion'; return; }
                input.value = body.name;
                owner = 'llm';
                msg.textContent = 'LLM suggestion: edit it or press Save.';
            }
        });
        input.addEventListener('keydown', e => {
            if (e.key === 'Enter') save(input.value.trim());
            if (e.key === 'Escape') close();
        });
    }

    // One editable name per row; the LLM fills them in on request and nothing is saved
    // until Apply. Rows are `{fids, name}`; unchanged and empty names are skipped.
    function openBatch(rows, auto) {
        const box = overlay(`<div style="background:var(--bg-card,#1e1e1e);padding:16px;border-radius:8px;width:min(560px,92vw);max-height:80vh;display:flex;flex-direction:column;gap:8px">
            <b>Rename ${rows.length} function${rows.length === 1 ? '' : 's'}</b>
            <div style="overflow:auto;display:flex;flex-direction:column;gap:6px">${rows.map((r, i) => `<label style="display:flex;flex-direction:column;gap:2px"><span class="dim" style="font-size:.8em">${escapeHtml(r.name)} <i class="fa-solid fa-spinner fa-spin" data-spin hidden></i></span><input type="text" maxlength="64" data-i="${i}" value="${escapeAttr(r.name)}" style="padding:4px"></label>`).join('')}</div>
            <div class="dim" data-msg style="min-height:1.2em"></div>
            <div style="display:flex;gap:8px;justify-content:flex-end">
                <button data-ai title="Ask the LLM for a name for every row">AI suggest all</button>
                <button data-cancel>Cancel</button>
                <button data-save>Apply</button>
            </div></div>`);
        const inputs = [...box.querySelectorAll('input')], msg = box.querySelector('[data-msg]');
        const owners = rows.map(() => 'user');
        let closed = false;
        inputs.forEach((inp, i) => inp.addEventListener('input', () => { owners[i] = 'user'; }));
        const close = () => { closed = true; box.remove(); };
        const spins = [...box.querySelectorAll('[data-spin]')], ai = box.querySelector('[data-ai]');
        const suggest = async () => {
            if (ai.disabled) return;
            ai.disabled = true;
            spins.forEach(sp => { sp.hidden = false; });
            msg.innerHTML = '<i class="fa-solid fa-spinner fa-spin"></i> LLM: 0/' + rows.length;
            let next = 0, finished = 0, failed = 0;
            const worker = async () => {
                while (!closed && next < rows.length) {
                    const i = next++;
                    inputs[i].disabled = true;
                    try {
                        const { ok, body } = await post('/api/notes/rename/suggest', { func_ids: rows[i].fids });
                        if (!ok || !body.name) throw new Error(body.error || 'no name');
                        inputs[i].value = body.name;
                        owners[i] = 'llm';
                    } catch (err) {
                        failed++;
                        inputs[i].title = String(err.message || err);
                    }
                    inputs[i].disabled = false;
                    spins[i].hidden = true;
                    finished++;
                    if (!closed) msg.innerHTML = `<i class="fa-solid fa-spinner fa-spin"></i> LLM: ${finished}/${rows.length}${failed ? `, ${failed} failed` : ''}`;
                }
            };
            await Promise.all([worker(), worker()]);
            ai.disabled = false;
            if (!closed) msg.textContent = `LLM: ${finished}/${rows.length}${failed ? `, ${failed} failed` : ''} - edit the names, then Apply.`;
        };
        const apply = async () => {
            const jobs = rows.map((r, i) => ({ fids: r.fids, name: inputs[i].value.trim(), owner: owners[i], was: r.name })).filter(j => j.name && j.name !== j.was);
            if (!jobs.length) return close();
            const { ok, body } = await post('/api/notes/rename', { renames: jobs.map(j => ({ func_ids: j.fids, name: j.name, owner: j.owner })) });
            if (!ok) { msg.textContent = body.error || 'Rename failed'; return; }
            close();
            done(jobs.map(j => ({ fids: j.fids, name: j.name })));
        };
        box.addEventListener('click', e => {
            if (e.target === box || e.target.closest('[data-cancel]')) return close();
            if (e.target.closest('[data-save]')) return apply();
            if (e.target.closest('[data-ai]')) return suggest();
        });
        box.addEventListener('keydown', e => { if (e.key === 'Escape') close(); });
        if (auto) suggest();
    }

    // The N-way panel redraws itself on this event; every other view re-reads its data.
    document.addEventListener('function-renamed', () => {
        if (!(window.NwayPanel && NwayPanel.current) && typeof window.refreshData === 'function') window.refreshData();
    });

    // What a function is called on screen: the analyst name when there is one.
    // Who named it: a small user / robot dot after the name.
    window.nameBadge = m => {
        if (!m || !m.user_name) return '';
        const llm = m.user_name_source === 'llm';
        return `<span class="nway-by ${llm ? 'llm' : 'user'}" title="${llm ? 'Named by the LLM' : 'Named by an analyst'}"><i class="fa-solid ${llm ? 'fa-robot' : 'fa-user-pen'}"></i></span>`;
    };
    window.fnName = m => (m && (m.user_name || m.function_name || m.name)) || '';
    window.FunctionRename = { open, openBatch };
})();
