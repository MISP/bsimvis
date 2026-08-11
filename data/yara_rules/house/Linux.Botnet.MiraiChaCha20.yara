/*
   Author: bsimvis (house rule)
   Date: 2026-08-11
   Reference: MalwareBazaar sample analyzed via bsimvis
   Notes: ARM32 Mirai-derivative botnet with DWARF debug info intact --
   source layout (attack_parser.c, attack_registry.c, chacha20.c,
   chacha20_table.c, tcp_brazilian_handshake.c, greip.c, udp_raknet.c,
   udp_openvpn.c) does not match vanilla leaked Mirai; ChaCha20-encrypted
   C2 traffic and the extra flood modules point to a maintained fork.
   Exact public family name unconfirmed -- rename `malware` once known.

   All three patterns are anchored to specific ARM32 instruction bytes
   pulled from disassembly of named (unstripped) functions, not from
   .rodata/.debug_str -- required so a match's file offset resolves to a
   Ghidra function via locateAddressesForFileOffset(), the same way this
   ruleset's function-level tagging works for every other rule here.
*/

rule ARM_ELF_Mirai_ChaCha20_Variant {
   meta:
      description = "ARM32 Mirai-derivative IoT botnet with ChaCha20-encrypted C2 (chacha20_xor byte-unpack prologue + shared raw-socket attack setup)"
      category = "botnet"
      malware = "MIRAI_CHACHA20_VARIANT"
      author = "bsimvis"
      reference = "MalwareBazaar"
      date = "2026-08-11"
      hash1 = "e2a883b35b094ce63315544ee8f66877"
      sha256_1 = "5905918ee6de9875209ffc756b520b30962f4e7f1f65aed09220e15b60553a96"
   strings:
      // chacha20_xor()'s key/nonce byte-unpack prologue -- struct field
      // offsets (+2, +3, +4, +8) are part of this fork's own ChaCha20
      // context layout, not generic ChaCha20 boilerplate.
      $chacha_xor_prologue = {
         F0 4F 2D E9 03 70 D0 E5 8B DF 4D E2 44 71 8D E5
         02 A0 D0 E5 40 A1 8D E5 04 C0 80 E2 03 B0 DC E5
         3C B1 8D E5 08 E0 80 E2 03 70 DE E5 02 B0 DC E5
      }

      // socket(AF_INET, SOCK_RAW, IPPROTO_TCP) setup shared by the raw
      // TCP flood modules (flood_ack, flood_syndata, ...), immediately
      // before the socket() call.
      $flood_socket_setup = {
         F0 4F 2D E9 5F DE 4D E2 00 A0 A0 E1 03 10 A0 E3
         04 D0 4D E2 02 00 A0 E3 06 20 A0 E3
      }

      // flood_udpraw()'s attack-buffer growth check ahead of its own
      // socket() setup.
      $flood_udpraw_setup = {
         F0 4F 2D E9 04 10 90 E5 5B 2E A0 E3 10 30 91 E5
         04 20 82 E2 02 00 53 E1 5F DE 4D E2 10 20 81 C5
         0C D0 4D E2 00 70 A0 E1 03 10 A0 E3 02 00 A0 E3
      }
   condition:
      uint16(0) == 0x457f and 2 of them
}
