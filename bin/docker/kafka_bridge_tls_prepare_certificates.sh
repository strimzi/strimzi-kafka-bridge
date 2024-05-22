#!/usr/bin/env bash

# Parameters:
# $1: Path to the new truststore
# $2: Truststore password
# $3: Public key to be imported
# $4: Alias of the certificate
function create_truststore {
   # Disable FIPS if needed
   if [ "$FIPS_MODE" = "disabled" ]; then
       KEYTOOL_OPTS="${KEYTOOL_OPTS} -J-Dcom.redhat.fips=false"
   else
       KEYTOOL_OPTS=""
   fi

   # shellcheck disable=SC2086
   keytool ${KEYTOOL_OPTS} -keystore $1 -storepass $2 -noprompt -alias $4 -import -file $3 -storetype PKCS12
}

# Parameters:
# $1: Path to the new keystore
# $2: Truststore password
# $3: Public key to be imported
# $4: Private key to be imported
# $5: Alias of the certificate
function create_keystore {
   RANDFILE=/tmp/.rnd openssl pkcs12 -export -in $3 -inkey $4 -name $5 -password pass:$2 -out $1 -certpbe aes-128-cbc -keypbe aes-128-cbc -macalg sha256
}

# Parameters:
# $1: Path to the new truststore
# $2: Truststore password
# $3: Base path where the certificates are mounted
# $4: Environment variable defining the certs that should be loaded
function prepare_truststore {
    TRUSTSTORE=$1
    PASSWORD=$2
    BASEPATH=$3
    TRUSTED_CERTS=$4

    rm -f "$TRUSTSTORE"

    IFS=';' read -ra CERTS <<< "${TRUSTED_CERTS}"
    for cert in "${CERTS[@]}"
    do
        for file in $BASEPATH/$cert
        do
            if [ -f "$file" ]; then
                echo "Adding $file to truststore $TRUSTSTORE with alias $file"
                create_truststore "$TRUSTSTORE" "$PASSWORD" "$file" "$file"
            fi
        done
    done
}

if [ -n "$KAFKA_BRIDGE_TRUSTED_CERTS" ]; then
    echo "Preparing Bridge truststore"
    prepare_truststore "/tmp/strimzi/bridge.truststore.p12" "$CERTS_STORE_PASSWORD" "${STRIMZI_HOME}/bridge-certs" "$KAFKA_BRIDGE_TRUSTED_CERTS"
fi

if [ -n "$KAFKA_BRIDGE_TLS_AUTH_CERT" ] && [ -n "$KAFKA_BRIDGE_TLS_AUTH_KEY" ]; then
    echo "Preparing keystore"
    rm -f "/tmp/strimzi/bridge.keystore.p12"
    create_keystore "/tmp/strimzi/bridge.keystore.p12" "$CERTS_STORE_PASSWORD" "${STRIMZI_HOME}/bridge-certs/$KAFKA_BRIDGE_TLS_AUTH_CERT" "${STRIMZI_HOME}/bridge-certs/$KAFKA_BRIDGE_TLS_AUTH_KEY" "$KAFKA_BRIDGE_TLS_AUTH_CERT"
    echo "Preparing keystore is complete"
fi

if [ -n "$KAFKA_BRIDGE_OAUTH_TRUSTED_CERTS" ]; then
    echo "Preparing OAuth truststore"
    prepare_truststore "/tmp/strimzi/oauth.truststore.p12" "$CERTS_STORE_PASSWORD" "/opt/strimzi/oauth-certs" "$KAFKA_BRIDGE_OAUTH_TRUSTED_CERTS"
fi
