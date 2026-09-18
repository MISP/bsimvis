# BSim Signature Settings

`DecompInterface.setSignatureSettings(mask)` controls how the Ghidra decompiler generates BSim feature hashes for a function. BSimVis sets it in `bsimvis/app/services/ghidra_service.py`.

## 1. Mask Layout

Bit 0 is a check bit; the modifier flags start at bit 2. The decompiler computes `sigmods = setting >> 2` (Ghidra `Ghidra/Features/Decompiler/src/decompile/cpp/signature.cc:933`). A value of `0` is rejected.

Note that `GraphSigManager::testSettings` only *permits* bit 0 — it does not require it. Validation is exactly "non-zero, and no bit set outside the allowed mask", so `0x4C` (`0x4D` without the check bit) also passes. Ghidra's own configurations always set it.

`GraphSigManager::testSettings` (`signature.cc:914-924`) permits only:

```
((SIG_COLLAPSE_SIZE | SIG_COLLAPSE_INDNOISE | SIG_DONOTUSE_CONST |
  SIG_DONOTUSE_INPUT | SIG_DONOTUSE_PERSIST) << 2) | 1   ==   0x1CD
```

Any bit outside that set is an error.

## 2. Flags

Source: `signature.hh:269-274`.

| Flag | Value | Meaning |
| --- | --- | --- |
| `SIG_COLLAPSE_SIZE` | `0x1` | Treat certain varnode sizes as the same |
| `SIG_COLLAPSE_INDNOISE` | `0x2` | Collapse varnodes that are indirect copies of each other |
| `SIG_DONOTUSE_CONST` | `0x10` | Do not use the value of a constant in the hash |
| `SIG_DONOTUSE_INPUT` | `0x20` | Do not use (fact of) being an input in the hash |
| `SIG_DONOTUSE_PERSIST` | `0x40` | Do not use (fact of) being a global in the hash |

## 3. Decoding `0x4D`

`0x4D` is the value BSimVis uses. In binary, `0b1001101`: the check bit is set, and `sigmods = 0x4D >> 2 = 0x13`, that is

```
SIG_COLLAPSE_SIZE | SIG_COLLAPSE_INDNOISE | SIG_DONOTUSE_CONST
```

This is the "nosize" configuration: varnode sizes are collapsed, so 32-bit and 64-bit builds of the same source can match. Contrast `0x49`, the same set minus `SIG_COLLAPSE_SIZE` — the "sized" configuration.

## 4. Constants Never Enter the Hash

`SIG_DONOTUSE_CONST` is set in every Ghidra BSim configuration, so constant *values* never contribute to feature hashes. Two builds that differ only in embedded constant data — an encrypted config blob, for example — produce identical signatures.

## 5. Lifecycle Warning

This mask changes the feature hashes *themselves*, at extraction time. Changing it invalidates every stored feature vector and requires a full re-decompilation of the collection.

This is the opposite of feature *weights*, which only re-interpret hashes that already exist. Weights can be swapped freely with no re-ingest.

## 6. Weights Tables

Ghidra ships weight tables in `Ghidra/Features/BSim/data/lshweights_*.xml`. Each declares the settings mask it belongs to via `<weights settings="0x...">`, and a table must only be used with the matching mask.

The mask alone does not identify a file: `lshweights_32.xml`, `lshweights_64.xml`, `lshweights_64_32.xml` and `lshweights_cpool.xml` all declare `0x49`, while only `lshweights_nosize.xml` declares `0x4d`. Ghidra resolves the pairing through database template configs such as `data/medium_nosize.xml`, which holds `<settings>0x4d</settings>`, `<k>17</k>`, `<L>146</L>` and `<weightsfile>lshweights_nosize.xml</weightsfile>`.

BSimVis therefore makes the mask/weights pairing explicit in configuration rather than inferring it from the mask.
