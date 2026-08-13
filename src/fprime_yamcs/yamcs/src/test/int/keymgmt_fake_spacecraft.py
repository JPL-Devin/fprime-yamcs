"""End-to-end integration test 'spacecraft' for the SDLS key management
service (see README "SDLS Key Management" and test/int/README.md).

Listens for TC frames on UDP 50001, decrypts them with the current SDLS
key (starting from the pre-shared key), reassembles the fragmented KEM
ciphertext, decapsulates it with the spacecraft ML-KEM-768 private key,
decrypts the OTAR PDU to recover the AES session key, and finally
verifies that post-rekey frames are encrypted with the NEW session key —
proving the ground SDLS SA was rekeyed in sync.

Usage:
    python3 keymgmt_fake_spacecraft.py <mlkem768-private-key.pem> \
        <initial-sdls-key-file> [openssl-binary]
"""
import hashlib
import socket
import struct
import subprocess
import sys
import tempfile
import os

from cryptography.hazmat.primitives.ciphers.aead import AESGCM

PRIV = sys.argv[1]
INITIAL_KEY = open(sys.argv[2], "rb").read()
OPENSSL = sys.argv[3] if len(sys.argv) > 3 else "openssl"

KEM_APID = 0x20
EP_APID = 0x21

sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
sock.bind(("127.0.0.1", 50001))
sock.settimeout(120)


def decrypt_frame(frame, key):
    """Decrypt a SDLS-protected TC transfer frame; returns (spi, data)."""
    frame_len = (struct.unpack(">H", frame[2:4])[0] & 0x3FF) + 1
    frame = frame[:frame_len]
    spi = struct.unpack(">H", frame[5:7])[0]
    iv = frame[7:19]
    ct_tag = frame[19:frame_len - 2]  # data + 16B MAC, excluding CRC16
    # AAD per YAMCS StandardAuthMask.TC (no segment header): VCID bits + SPI
    aad = bytes([0, 0, frame[2] & 0xFC, 0, 0, frame[5], frame[6]]) + b"\x00" * 12
    data = AESGCM(key).decrypt(iv, ct_tag, aad)
    return spi, data


def parse_space_packets(data):
    """Yield (apid, payload) for each space packet concatenated in a frame."""
    while len(data) >= 6:
        apid = struct.unpack(">H", data[0:2])[0] & 0x7FF
        length = struct.unpack(">H", data[4:6])[0] + 1
        yield apid, data[6:6 + length]
        data = data[6 + length:]


kem_frags, kem_frag_count = {}, None
kem_ct = otar = activation = verification = None

while not (kem_ct and otar and activation and verification):
    frame, _ = sock.recvfrom(4096)
    spi, data = decrypt_frame(frame, INITIAL_KEY)
    print(f"frame {len(frame)}B SPI={spi} decrypted with INITIAL key")
    for apid, payload in parse_space_packets(data):
        if apid == KEM_APID:
            frag_index, kem_frag_count = payload[3], payload[4]
            kem_frags[frag_index] = payload[5:]
            if len(kem_frags) == kem_frag_count:
                kem_ct = b"".join(kem_frags[i] for i in range(kem_frag_count))
                print(f"  KEM ciphertext reassembled: {len(kem_ct)}B")
        elif apid == EP_APID:
            tag = payload[0]
            bits = struct.unpack(">H", payload[1:3])[0]
            body = payload[3:3 + bits // 8]
            print(f"  EP PDU tag=0x{tag:02x}")
            if tag == 0x01:
                otar = body
            elif tag == 0x02:
                activation = body
            elif tag == 0x04:
                verification = body

# Decapsulate the KEM ciphertext -> shared secret (transaction KEK)
with tempfile.TemporaryDirectory() as tmp:
    ct_file, ss_file = os.path.join(tmp, "ct.bin"), os.path.join(tmp, "ss.bin")
    with open(ct_file, "wb") as f:
        f.write(kem_ct)
    subprocess.run([OPENSSL, "pkeyutl", "-decap", "-inkey", PRIV,
                    "-in", ct_file, "-secret", ss_file], check=True)
    kek = open(ss_file, "rb").read()

# Decrypt the OTAR PDU: masterKeyId(2) | IV(12) | {keyId(2), key(32)}+MAC(16)
iv, ct_tag = otar[2:14], otar[14:]
plaintext = AESGCM(kek).decrypt(iv, ct_tag, None)
session_key_id = struct.unpack(">H", plaintext[0:2])[0]
session_key = plaintext[2:]
print(f"OTAR recovered sessionKeyId={session_key_id} "
      f"key sha256={hashlib.sha256(session_key).hexdigest()[:16]}")

act_key_id = struct.unpack(">H", activation[0:2])[0]
ver_key_id = struct.unpack(">H", verification[0:2])[0]
assert act_key_id == session_key_id == ver_key_id

# Any frame after rekey must decrypt with the NEW session key only
print("waiting for post-rekey frame (trigger a second rekey)...")
frame, _ = sock.recvfrom(4096)
try:
    decrypt_frame(frame, INITIAL_KEY)
    print("FAIL: post-rekey frame still decrypts with the old key")
    sys.exit(1)
except Exception:
    pass
spi, data = decrypt_frame(frame, session_key)
print(f"post-rekey frame decrypted with NEW session key (SPI={spi})")
print("SDLS INTEGRATION TEST PASS")
