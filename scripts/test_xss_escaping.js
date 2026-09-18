// Guards the escaping of sample-derived values in the UI.
//
// Every field below arrives from an uploaded sample or its analysis -- an
// upload's `file_name` is a query parameter, `function_name` and `language_id`
// are read out of the binary, `avtype` / `yara` / `cc_ip` come from its
// metadata -- and every view builds its markup as a template literal handed to
// `innerHTML`. So each of these values must go through `escapeHtml` (text) or
// `escapeAttr` (attribute) on the way out.
//
// This checks the source rather than the rendered output: the sinks are buried
// inside render methods that need d3 and a live DOM to reach, and a scan also
// covers views added after this was written.
//
// Run: node scripts/test_xss_escaping.js
const fs = require('fs');
const path = require('path');
const assert = require('assert');

const JS_DIR = path.join(__dirname, '..', 'bsimvis', 'app', 'static', 'js');

// Values carrying attacker-controlled content.
const TAINTED = /(file_name|file_names|path_in_parent|avtype|yara|cc_ip|function_name|func_name|cluster_name|language_id|\bfile_md5)/i;
// Escapers, and the helpers that escape internally (verified by hand).
const ESCAPED = /escapeHtml|escapeAttr|jsString|safeCss|encodeURI|renderFileName|renderFunction|renderTag|renderMd5|renderNoteButton|formatArray|\blabel\(|\besc\(|FunctionFilters\.|middleTruncate|\.length\b|toFixed|Number\(|parseInt/;
// A template literal that is emitting markup, not building a URL or an object.
const MARKUP = /<\w|<\/\w|\w+="|\w+='/;

function jsFiles(dir) {
    return fs.readdirSync(dir, { withFileTypes: true }).flatMap(e => {
        const full = path.join(dir, e.name);
        // Skip third-party code and any worktree checked out under the tree.
        if (e.isDirectory()) return (e.name === 'vendor' || e.name.startsWith('.')) ? [] : jsFiles(full);
        return e.name.endsWith('.js') ? [full] : [];
    });
}

// ---- the escapers themselves --------------------------------------------
// Everything below trusts these, so check they bite before trusting them.
const utils = {};
new Function('exports', `${fs.readFileSync(path.join(JS_DIR, 'utils.js'), 'utf8')
    .match(/function escapeHtml[\s\S]*?\n}/)[0]}
    exports.escapeHtml = escapeHtml;`)(utils);
const PAYLOAD = '"><img src=x onerror=alert(1)>';
assert.ok(!utils.escapeHtml(PAYLOAD).includes('<img'), 'escapeHtml neutralises a tag');
assert.ok(!utils.escapeHtml(PAYLOAD).includes('"'), 'escapeHtml neutralises a quote');
assert.strictEqual(utils.escapeHtml('a.elf'), 'a.elf', 'an ordinary value is untouched');
console.log('ok  escapers');

// ---- no raw sink anywhere in the views -----------------------------------
const raw = [];
for (const file of jsFiles(JS_DIR)) {
    const lines = fs.readFileSync(file, 'utf8').split('\n');
    lines.forEach((line, i) => {
        if (!MARKUP.test(line)) return;
        for (const m of line.matchAll(/\$\{([^{}]*(?:\{[^{}]*\}[^{}]*)*)\}/g)) {
            const expr = m[1].trim();
            if (ESCAPED.test(expr)) continue;
            // A field name written as a string literal -- `icon('cluster_name')`
            // -- is not the field's value, so blank the literals out first.
            const stripped = expr.replace(/'[^']*'|"[^"]*"|`[^`]*`/g, "''");
            if (!TAINTED.test(stripped)) continue;
            // A ternary picking between two literals emits a literal.
            if (/\?\s*''\s*:\s*''\s*$/.test(stripped)) continue;
            raw.push(`${path.relative(path.join(__dirname, '..'), file)}:${i + 1}: \${${expr}}`);
        }
    });
}
assert.deepStrictEqual(raw, [], `sample-derived values reach innerHTML unescaped:\n  ${raw.join('\n  ')}`);
console.log('ok  view sinks');

console.log('ok');
