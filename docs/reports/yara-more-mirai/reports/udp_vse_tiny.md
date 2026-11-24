# udp_vse_tiny

Rule: `Linux_Capability_UdpVseTinyFlood` (capability, low). Recall 2/2 unpacked, 0 hits outside; the 2 UPX originals are not matched (no strings).

## Evidence
Coherent but tiny: two builds (MIPS BE 41300cb2..., MIPS LE 7a4879ef...) of one 15-function program. The whole string table is the method list `UDP-RAW UDP-CUSTOM UDP-VSE TCP-FIVEM TCP-HANDSHAKE TCP-ACK MINECRAFT`, `STOP`, the format `%s %s %d %d` and the address `212.43.149.186` (likely the controller, unverified). Method names suggest game-server floods (Valve VSE, Minecraft, FiveM). No scanner, no killer, no persistence: it is a flood module, not a full bot. The boatnet.* names of the packed originals are misleading.

## Measured
Rule (5 of 6 method names + the IP): target 2/2 unpacked (2/4 incl. UPX originals), 0 of 411 unrelated.

## Gaps
Packed originals need the unpacked sibling. Two samples of one build only; the IP anchor will not survive a re-pointed rebuild, but the 5-of-6 method table would.
