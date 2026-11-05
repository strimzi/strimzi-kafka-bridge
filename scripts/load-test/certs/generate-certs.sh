#!/usr/bin/env bash
#
# Generate the PKI material for the mTLS and DPoP load-test scenarios:
#   - a local CA
#   - the bridge certificate (HTTPS server + TLS client for the Kafka SSL listener)
#   - a load-test client certificate (for k6 mTLS, subject CN=loadtest-user)
#   - the Kafka broker keystore/truststore (PKCS12) for the SSL listener
#   - an EC P-256 key pool for the DPoP proofs (exported as JSON for the k6 signer)
#
# The material is generated into ./generated (git-ignored). Re-running the script
# regenerates everything from scratch.
#
set -euo pipefail

DIR="$(cd "$(dirname "$0")" && pwd)/generated"
PASSWORD="certs-password"
DAYS=30

mkdir -p "$DIR"
rm -f "$DIR"/*

echo "-> CA"
openssl req -x509 -newkey rsa:2048 -nodes -keyout "$DIR/ca-key.pem" -out "$DIR/ca-cert.pem" \
    -days "$DAYS" -subj "/CN=Loadtest CA" 2>/dev/null

issue() { # issue <name> <CN> <SAN extensions file content>
    local name="$1" cn="$2" ext="$3"
    openssl req -newkey rsa:2048 -nodes -keyout "$DIR/$name-key.pem" -out "$DIR/$name.csr" \
        -subj "/CN=$cn" 2>/dev/null
    printf "%s" "$ext" > "$DIR/$name.ext"
    openssl x509 -req -in "$DIR/$name.csr" -CA "$DIR/ca-cert.pem" -CAkey "$DIR/ca-key.pem" \
        -CAcreateserial -out "$DIR/$name-cert.pem" -days "$DAYS" \
        -extfile "$DIR/$name.ext" 2>/dev/null
    rm "$DIR/$name.csr" "$DIR/$name.ext"
}

echo "-> bridge certificate (HTTPS server + Kafka TLS client)"
issue bridge "bridge" \
    "subjectAltName=DNS:localhost,IP:127.0.0.1
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth,clientAuth"

echo "-> load-test client certificate (CN=loadtest-user, for k6 mTLS)"
issue loadtest-client "loadtest-user" \
    "basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=clientAuth"

echo "-> Kafka broker certificate"
issue kafka "kafka" \
    "subjectAltName=DNS:localhost,IP:127.0.0.1
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth,clientAuth"

echo "-> Kafka PKCS12 keystore and truststore"
openssl pkcs12 -export -in "$DIR/kafka-cert.pem" -inkey "$DIR/kafka-key.pem" \
    -certfile "$DIR/ca-cert.pem" -out "$DIR/kafka.keystore.p12" -passout "pass:$PASSWORD" 2>/dev/null
keytool -importcert -noprompt -file "$DIR/ca-cert.pem" -alias ca \
    -keystore "$DIR/kafka.truststore.p12" -storetype PKCS12 -storepass "$PASSWORD" >/dev/null

echo "-> bridge PKCS12 keystore (TLS client certificate for the Kafka SSL listener)"
openssl pkcs12 -export -in "$DIR/bridge-cert.pem" -inkey "$DIR/bridge-key.pem" \
    -certfile "$DIR/ca-cert.pem" -out "$DIR/bridge.keystore.p12" -passout "pass:$PASSWORD" 2>/dev/null

echo "-> DPoP key pairs (EC P-256 for ES256 proofs, exported as a JSON array for the k6 proof signer)"
# a pool of keys, so that the k6 USERS knob can simulate distinct DPoP senders
DPOP_KEY_COUNT="${DPOP_KEY_COUNT:-10}"
rm -f "$DIR"/dpop-key-*.pem
python3 - "$DIR" "$DPOP_KEY_COUNT" << 'EOF'
import json, subprocess, sys

dir_path, count = sys.argv[1], int(sys.argv[2])

def read_tlv(data, offset):
    # minimal ASN.1 TLV reader: returns (tag, value bytes, next offset)
    tag = data[offset]
    offset += 1
    length = data[offset]
    offset += 1
    if length & 0x80:
        n = length & 0x7F
        length = int.from_bytes(data[offset:offset + n], "big")
        offset += n
    return tag, data[offset:offset + length], offset + length

def key_components(pem_path):
    # parse the SEC1 ECPrivateKey DER structure:
    # SEQUENCE { INTEGER 1, OCTET STRING d (32 bytes),
    #            [0] OID prime256v1, [1] BIT STRING public key (00 04 || X(32) || Y(32)) }
    der = subprocess.run(["openssl", "ec", "-in", pem_path, "-outform", "DER"],
                         capture_output=True, check=True).stdout
    tag, body, _ = read_tlv(der, 0)              # outer SEQUENCE
    _, _, offset = read_tlv(body, 0)             # version INTEGER
    tag, d, offset = read_tlv(body, offset)      # OCTET STRING: private scalar
    assert tag == 0x04 and len(d) == 32
    tag, _, offset = read_tlv(body, offset)      # [0]: curve OID
    assert tag == 0xA0
    tag, wrapped, offset = read_tlv(body, offset)  # [1]: BIT STRING
    assert tag == 0xA1
    tag, point, _ = read_tlv(wrapped, 0)
    assert tag == 0x03 and point[0] == 0x00 and point[1] == 0x04
    x, y = point[2:34], point[34:66]
    return d, x, y

keys = []
for i in range(1, count + 1):
    pem_path = f"{dir_path}/dpop-key-{i}.pem"
    subprocess.run(["openssl", "ecparam", "-name", "prime256v1", "-genkey", "-noout", "-out", pem_path],
                   capture_output=True, check=True)
    d, x, y = key_components(pem_path)
    keys.append({
        # EC P-256 key components (hex) for the pure-JS ES256 signer of the k6 scripts
        "kty": "EC",
        "crv": "P-256",
        "d": d.hex(),
        "x": x.hex(),
        "y": y.hex(),
    })

with open(f"{dir_path}/dpop-keys.json", "w") as out:
    json.dump(keys, out)
EOF

echo "-> done: $DIR"
# the Kafka broker container reads the keystore/truststore as the "kafka" user
chmod 644 "$DIR"/*
ls -l "$DIR"
