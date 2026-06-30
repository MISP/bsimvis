# Plan: Fix "undefined/functions/undefined" in Diff Queue

## Root Cause

When `addToDiff(id, name)` is called (from context menus, buttons, feature views, etc.) with a single function ID (e.g. `main:func:58e235ab26b8c4531ef869403f120993:140001b27`), the `addToDiff` function in `diff_queue.js` parses it via `normalizeFuncId` and then manually extracts fields in lines 149-159:

```javascript
const parts = id.split(':');
const obj = {
    id: id,
    name: name,
    collection_a: stripPoolPrefix(parts[0]) || 'main',
    md5_a: parts[2] || '',
    addr_a: parts[3] || '',
    collection_b: '',   // <-- EMPTY
    md5_b: '',          // <-- EMPTY
    addr_b: '',         // <-- EMPTY
    pool: ...
};
```

When both items end up in the diff queue, each item's side-B fields are empty strings `""`. `openStandaloneDiff()` then constructs:

```
/collections/main/files/58e235ab26b8c4531ef869403f120993/functions/140001b27/vs/main/undefined/undefined
                              ^-- md5_b="" becomes "undefined" string in template literal
                                         ^-- addr_b="" becomes "undefined" string
```

The `undefined` in the URL is **not** `undefined` the JS value -- it's because `encodeURIComponent('')` produces `''`, but somewhere the fallback `|| ''` doesn't fire, and the empty string passes through to URL building where it renders as `undefined` in the path segments.

## What Gets Broken

Every place that constructs a diff URL or a diff queue entry depends on `collection_b`, `md5_b`, `addr_b` being populated. Any flow where:
- A single function is added first (or only)
- The queue is loaded from localStorage and the stored entry has empty side-B fields

...will produce broken URLs.

Affected entry points:
1. **diff_queue.js `addToDiff()`** (line 150-159) -- the primary source
2. **diff_queue.js `openStandaloneDiff()`** (line 372-397) -- builds the URL from queue entries
3. **diff_queue.js `buildDiffUrl()`** (line 79-93) -- same pattern
4. **diff_queue.js `showDiffPreview()`** (line 508-522) -- preview fetches
5. **dashboard.js `openDiffDirectly()`** -- callers/callees navigation
6. **context_menu.js** (line 308) -- "Add to Diff" from context menu passes `norm.id` which may have empty collection/md5 derived from `parseFuncId`
7. **metadata.js** (line 106) -- button onclick passes `fullId` which may be malformed
8. **entity_renderer.js** (line 52) -- `renderFunction` passes `funcId` built from `f['function_id']` or template -- may miss collection

## Fix Strategy

### Phase 1: Core Fix -- normalizeFuncId (diff_queue.js)

Make `normalizeFuncId` robust by ensuring every parsed ID has all fields filled. If `collection_b`, `md5_b`, `addr_b` are absent (meaning it's a single-item ID), copy them from the "a" side.

**File**: `bsimvis/app/static/js/diff_queue.js`

In `normalizeFuncId(id)` (line 60-77), when the ID is not already in `:func:` or `:function:` format, ensure all fields are populated:

```javascript
function normalizeFuncId(id) {
    if (!id || typeof id !== 'string') return id;
    if (id.includes(':function:') || id.includes(':func:')) return id;
    const parts = id.split(':');
    if (parts.length >= 4) {
        const addrPart = parts.pop();
        const funcPart = parts.pop();
        const emptyPart = parts.pop();
        const md5Part = parts.pop();
        const colPart = parts.join(':');
        if (addrPart && addrPart.startsWith('@') && md5Part && md5Part.startsWith('#')) {
            const cleanAddr = addrPart.substring(1);
            const cleanMd5 = md5Part.substring(1);
            return `${colPart}:func:${cleanMd5}:${cleanAddr}`;
        }
    }
    return id;
}
```

But this function doesn't have full data. The real fix needs to happen in `parseFuncIdFromStr` which has access to the full object. **Actually, the fix belongs in `addToDiff`**:

### Phase 1A: Fix addToDiff (diff_queue.js line 140-183)

In `addToDiff`, when the incoming `a1` resolves to a single-item ID where side-B fields are empty, populate them from side-A:

```javascript
// After parsing id and building obj, fix up side-B if still empty:
if (!obj.md5_b || !obj.addr_b) {
    obj.collection_b = obj.collection_a;
    obj.md5_b = obj.md5_a;
    obj.addr_b = obj.addr_a;
}
```

This is the minimal, safest single-change fix.

### Phase 1B: Fix buildDiffUrl (diff_queue.js line 79-93)

In `buildDiffUrl`, the `stripFuncId` return object uses fallback defaults with wrong key names (`f1.collection` vs `f1.collection_a`). Fix the fallback object keys and add the same copy logic:

```javascript
function buildDiffUrl(id1, id2) {
    const f1 = stripFuncId(id1) || { collection_a: '', collection_b: '', md5_a: '', addr_a: '', collection: 'main', md5: '', address: '' };
    const f2 = stripFuncId(id2) || { collection_a: '', collection_b: '', md5_a: '', addr_a: '', collection: 'main', md5: '', address: '' };

    let collA = f1.collection_a || (f1.collection ? stripPoolPrefix(f1.collection) : 'main');
    let collB = f2.collection_a || (f2.collection ? stripPoolPrefix(f2.collection) : 'main');
    
    // If side-B fields are empty, copy from side-A (single-item fix)
    const md5A = f1.md5_a || (f1.hasOwnProperty('md5') ? stripPoolPrefix(f1.md5) : '');
    const addrA = f1.addr_a || f1.address || '';
    const md5B = f2.md5_b || (f2.hasOwnProperty('md5') ? stripPoolPrefix(f2.md5) : '');
    const addrB = f2.addr_b || f2.address || '';
    
    // For single-item (same ID twice), side-B = side-A
    if (!md5B || !addrB) {
        // ... handle correctly
    }
    ...
}
```

Actually the **root cause** is simpler. `stripFuncId` calls `parseFuncIdFromStr` which returns `{collection_b: col || '', ...}` for `:func:` format. The issue is that `:func:` IDs like `main:func:md5:addr` parsed by `parseFuncIdFromStr` line 25-29:

```javascript
if (id.includes(':func:')) {
    const parts = id.split(':func:');
    const col = stripPoolPrefix(parts[0]);
    const rest = parts[1] ? parts[1].split(':') : [];
    return { ...def, collection_a: col || '', md5_a: rest[0] || '', addr_a: rest[1] || '', collection_b: col || '', md5_b: rest[0] || '', addr_b: rest[1] || '' };
}
```

This actually **does** populate side-B correctly for `:func:` format! So the path must be through the flat format (line 38-46) or through the `addToDiff` line 149-159 path.

### Tracing the actual broken path

When `addToDiff(a1, a2)` is called:
```javascript
const id = normalizeFuncId(a1);  // This normalizes to "coll:func:md5:addr" or returns raw
const parts = id.split(':');
const obj = {
    collection_a: stripPoolPrefix(parts[0]) || 'main',
    md5_a: parts[2] || '',
    addr_a: parts[3] || '',
    collection_b: '',   // <-- ALWAYS empty
    md5_b: '',
    addr_b: '',
};
```

The bug is **definitively** in lines 150-160 of `addToDiff`. The fix is straightforward:

**In `addToDiff` (diff_queue.js line 159)**, after building `obj`:
```javascript
// Fix: for single-item IDs, side-B = side-A
if (!obj.md5_b || !obj.addr_b) {
    obj.collection_b = obj.collection_a;
    obj.md5_b = obj.md5_a;
    obj.addr_b = obj.addr_a;
}
```

### Phase 2: OpenStandaloneDiff & buildDiffUrl consistency

**In `openStandaloneDiff` (line 377-391)**, ensure URL building handles empty side-B fields:
```javascript
const colB = encodeURIComponent(p2.collection_b || p1.collection_a || 'main');
const md5B = encodeURIComponent(p2.md5_b || p1.md5_a || '');
const addrB = encodeURIComponent(p2.addr_b || p1.addr_a || '');
```

**In `buildDiffUrl` (line 79-93)**, the fallback defaults use wrong property names:
```javascript
// Line 80-81: fix fallback keys to match parseFuncIdFromStr output
const f1 = stripFuncId(id1) || { collection_a: '', collection_b: '', md5_a: '', addr_a: '', md5_b: '', addr_b: '' };
// Same for f2
```

### Phase 3: Context Menu ID Resolution (context_menu.js)

For the context menu "Add to Diff" (line 308), the `norm.id` value depends on what data was passed to `showGraphContextMenu`. For `type === 'function'` nodes (line 135-145):

```javascript
norm.id = data.id || data.function_id;
norm.name = data.name || data.function_name;
norm.addr = data.entrypoint || data.entrypoint_address || data.addr;
norm.md5 = data.md5 || data.file_md5;
if (norm.id && (!norm.addr || !norm.md5)) {
    const parsed = window.parseFuncId(norm.id);
    norm.addr = norm.addr || parsed.address;
    norm.md5 = norm.md5 || parsed.md5;
}
```

If `data.id` is already in `:func:` format, `norm.id` is fine. But if the graph node data doesn't include `file_md5` or `entrypoint_address`, and `parseFuncId` can't extract them, the ID might be partially populated. The `addToDiff` core fix (Phase 1) handles this.

However, there's also the **cluster member** path (line 110-129):
```javascript
if (type === 'cluster') {
    resolvedType = 'function';
    norm.id = data.id || data.function_id;
    ...
}
```

Here `norm.id` comes from `data.id` which might be a raw string like `file:func:md5:addr` with no collection. The ID format depends on the D3 graph data -- **but the `addToDiff` core fix handles this too** since it fills in side-B from side-A.

### Phase 4: Feature View ID Format (feature/index.html, feature_view.js)

In feature views, `funcId` is built as:
```javascript
let funcId = normalizeFuncId(occ['function_id']);
```

The `function_id` comes from the API. If the API returns `main:func:58e235ab:...`, this is fine. But if it returns something like `:func:58e235ab:140001b27` (missing collection prefix), `normalizeFuncId` won't detect it as a 4-part ID and will return it as-is.

The `addToDiff` core fix (Phase 1) handles this since `stripPoolPrefix('')` returns `''` which falls to `|| 'main'`.

### Phase 5: Entity Renderer Function ID (entity_renderer.js)

In `renderFunction` (line 22-24):
```javascript
let collection = stripPoolPrefix(f['collection'] || '');
const funcId = f['function_id'] || `${collection}:func:${file_md5}:${entry}`;
```

If `f['collection']` is missing or empty, `collection` becomes `''`. The fallback is `:func:md5:addr` which has no collection prefix. When this is passed to `addToDiff`, the first part after `split(':')` is `''`.

Fix: ensure `collection` defaults to the current routing context when missing:
```javascript
let collection = stripPoolPrefix(f['collection'] || '') || (typeof getCollectionFromHash === 'function' ? getCollectionFromHash() : 'main');
```

## File-by-File Change Summary

### 1. bsirvis/app/static/js/diff_queue.js (PRIMARY FIX)

**Line ~149-160 (addToDiff function)**: After constructing `obj`, add:
```javascript
// If side-B fields are empty (single-item ID), copy from side-A
if (!obj.md5_b || !obj.addr_b) {
    obj.collection_b = obj.collection_a;
    obj.md5_b = obj.md5_a;
    obj.addr_b = obj.addr_a;
}
```

**Line ~79-81 (buildDiffUrl fallback defaults)**: Fix the fallback object key names:
```javascript
const f1 = stripFuncId(id1) || { collection_a: '', collection_b: '', md5_a: '', addr_a: '', md5_b: '', addr_b: '' };
```

### 2. bsirvis/app/static/js/entity_renderer.js

**Line 22-24 (renderFunction)**: Ensure collection defaults to routing context:
```javascript
let collection = stripPoolPrefix(f['collection'] || '') || (typeof getCollectionFromHash === 'function' ? getCollectionFromHash() : 'main');
```

### 3. bsirvis/app/static/js/metadata.js

**Line 15 (renderFunctionMetadata)**: `const collection = parsed.collection || '';` may be empty if `parseEntityId` returns nothing. Fix:
```javascript
const collection = parsed.collection || (typeof getCollectionFromHash === 'function' ? getCollectionFromHash() : 'main');
```

**Line 106 (addToDiff button)**: The `fullId` passed to `addToDiff` must be verified to have a collection. If `parsed.collection` is empty, fix it before passing.

### 4. bsirvis/app/static/js/views/feature_view.js

The `funcId` built here relies on `occ['function_id']` from the API. This is fine if the API returns proper IDs. No change needed if Phase 1 core fix is applied.

### 5. bsirvis/app/static/feature/index.html

Same as above - relies on API data. No change needed with Phase 1 fix.

### 6. bsirvis/app/static/js/context_menu.js

The `norm.id` construction in `showGraphContextMenu` is the weakest link. When data comes from D3 graph nodes, `data.id` might be partial. The core fix in Phase 1 handles the downstream effect.

However, for robustness, in the `type === 'function'` block (lines 135-145), add:
```javascript
// Ensure norm.id has a collection prefix
if (norm.id && !norm.id.includes(':') || (norm.id && !norm.id.includes(':func:') && !norm.id.includes(':function:'))) {
    // Try to reconstruct with collection
    const col = typeof getCollectionFromHash === 'function' ? getCollectionFromHash() : 'main';
    if (norm.md5 && norm.addr) {
        norm.id = `${col}:func:${norm.md5}:${norm.addr}`;
    }
}
```

## Implementation Order

1. **diff_queue.js `addToDiff`** -- Phase 1A: The core fix (most impactful)
2. **diff_queue.js `buildDiffUrl`** -- Phase 1B: Fix fallback defaults
3. **entity_renderer.js `renderFunction`** -- Phase 5: Fix collection default
4. **metadata.js `renderFunctionMetadata`** -- Phase 4: Fix collection default
5. **context_menu.js `showGraphContextMenu`** -- Phase 6: Reconstruct norm.id
6. **Verify** all entry points produce correct URLs by testing diff from multiple views (context menu, feature view, file list, metadata card)

## Verification Test Plan

After applying fixes, test these scenarios:
1. Right-click "Add to Diff" on a function in the **function diff view** vs another function
2. Right-click "Add to Diff" from a **cluster graph** node
3. "Add to Diff" from **feature occurrences table**
4. "Add to Diff" from **function metadata card**
5. "Add to Diff" from **entity rendered in a table/list view**
6. Open a function's context menu in a **pool view** -- verify pool prefix is preserved
7. Verify all generated URLs produce the correct path structure (no `undefined` in path)

## Risk Assessment

- **Low risk**: The core fix in `addToDiff` (Phase 1A) is additive -- it only fills in defaults when fields are empty. It cannot break existing correct behavior.
- **Medium risk**: Changes to EntityRenderer/metadata.js change the function ID format for all rendered functions. The IDs will now always include a collection prefix, which is correct.
- **No risk** expected to existing functionality.
