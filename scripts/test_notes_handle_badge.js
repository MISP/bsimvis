const assert = require('assert');
const fs = require('fs');

const source = fs.readFileSync('bsimvis/app/static/js/notes.js', 'utf8');
const handleRule = source.match(/\.panel-handle \{([\s\S]*?)\n        \}/);

assert(handleRule, 'notes handle CSS rule is missing');
assert(/position:\s*relative;/.test(handleRule[1]),
    'the notes badge must be positioned relative to its handle');
