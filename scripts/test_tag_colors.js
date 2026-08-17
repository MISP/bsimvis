// Parity check between the tag-colour rule in `tag_taxonomy.py` and its browser
// mirror in `tag_color.js`. Two implementations of one rule is the whole risk
// here: the day they disagree, the same tag is two colours in two views and
// nothing errors. Both sides pin the same ids to the same output.
//
// The Python side asserts these vectors in `tag_taxonomy.demo()`; this asserts
// the JS side against the same numbers, and reads the parameters out of
// `tag_taxonomy.py` rather than restating them, so retuning the rule cannot
// leave a stale copy behind here.
// Run: node scripts/test_tag_colors.js
const fs = require('fs');
const path = require('path');
const assert = require('assert');
const { execFileSync } = require('child_process');

const root = path.join(__dirname, '..');
const src = fs.readFileSync(
    path.join(root, 'bsimvis', 'app', 'static', 'js', 'tag_color.js'),
    'utf8'
);

// The config normally arrives from `/api/tags/colors`; here it comes straight
// out of the module that defines it, which is the same JSON that endpoint
// returns.
// `tag_taxonomy` imports nothing outside the stdlib, so a bare interpreter is
// enough -- no venv needed to check the two rules agree. Loaded by file path
// rather than as `bsimvis.app.services.…`, because importing the package pulls
// in Flask.
const python = process.env.PYTHON || 'python3';
const cfg = JSON.parse(
    execFileSync(python, [
        '-c',
        'import json,importlib.util as u;' +
            's=u.spec_from_file_location("tt","bsimvis/app/services/tag_taxonomy.py");' +
            'm=u.module_from_spec(s);s.loader.exec_module(m);' +
            'print(json.dumps(m.color_config()))',
    ], { cwd: root }).toString()
);

// `tag_color.js` is a browser global script that fetches on load: run it with a
// stubbed `fetch` that resolves to the config above.
const win = {};
const fetchStub = () => Promise.resolve({ json: () => Promise.resolve(cfg) });
new Function('window', 'fetch', 'console', src)(win, fetchStub, console);
const TagColor = win.TagColor;

TagColor.ready.then(() => {
    // Same vectors as `tag_taxonomy.COLOR_VECTORS`, same expected output.
    const expected = [
        ['severity:high', 0, 1, 0],
        ['severity:low', 55, 1, 0],
        ['origin:lib:libc', 75.52, 0, 0],
        ['origin:lib:libc:2.31:memcpy', 75.52, 0, 2],
        ['category:network', 76.91, 0, 0],
        ['capa:host-interaction:file-system:write', 120.52, 0, 1],
        ['mitre:t1027.005', 237.27, 0, 1],
        ['cve:cve-2021-44228', 99.82, 0, 0],
    ];
    for (const [id, hue, tone, step] of expected) {
        assert.deepStrictEqual(TagColor.style(id), { hue, tone, step }, id);
    }

    // The properties the rule exists for, restated on the JS side so a mirror
    // that drifts structurally still fails even if the vectors were updated.
    assert.strictEqual(
        TagColor.style('category:network:c2').hue,
        TagColor.style('category:network').hue
    );
    assert.strictEqual(
        TagColor.style('mitre:t1027.005').hue,
        TagColor.style('mitre:t1027').hue
    );
    assert.deepStrictEqual(TagColor.style('bookmark'), TagColor.style('user:bookmark'));
    assert.strictEqual(TagColor.style('user:').hue, null);

    // The painted value: hue fixed per tag, saturation and lightness from the
    // theme, depth lightening on top. Grey drops the hue and keeps the shading.
    assert.strictEqual(
        TagColor.css('category:network'),
        'hsl(76.91, var(--tagc-s0), calc(var(--tagc-l0) + 0%))'
    );
    assert.strictEqual(
        TagColor.css('category:network:c2'),
        `hsl(76.91, var(--tagc-s0), calc(var(--tagc-l0) + ${cfg.step_lum}%))`
    );
    assert.ok(TagColor.css('category:network', { gray: true }).startsWith('hsl(0, 0%,'));

    // A hand-picked colour on a tag beats the derived one, and grey still wins
    // over both -- unmatched mass reports no tag agreement whatever the tag is.
    win.tagMetadata = { 'user:bookmark': { color: '#66d9ef' } };
    assert.strictEqual(TagColor.forTag('user:bookmark'), '#66d9ef');
    assert.strictEqual(TagColor.forTag('category:network'), TagColor.css('category:network'));
    assert.ok(TagColor.forTag('user:bookmark', { gray: true }).startsWith('hsl(0, 0%,'));

    console.log('tag colour parity OK');
}).catch(e => {
    console.error(e);
    process.exit(1);
});
