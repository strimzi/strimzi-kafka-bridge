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
   RANDFILE=/tmp/.rnd openssl pkcs12 -export -in $3 -inkey $4 -name $5 -password pass:$2 -out $1
}

# $1 = trusted certs, $2 = TLS auth cert, $3 = TLS auth key, $4 = truststore path, $5 = keystore path, $6 = certs and key path
trusted_certs=$1
tls_auth_cert=$2
tls_auth_key=$3
truststore_path=$4
keystore_path=$5
certs_key_path=$6

if [ -n "$trusted_certs" ]; then
    echo "Preparing truststore"
    rm -f "$truststore_path"
    IFS=';' read -ra CERTS <<< ${trusted_certs}
    for cert in "${CERTS[@]}"
    do
        create_truststore $truststore_path $CERTS_STORE_PASSWORD $certs_key_path/$cert $cert
    done
    echo "Preparing truststore is complete"
fi

if [ -n "$tls_auth_cert" ] && [ -n "$tls_auth_key" ]; then
    echo "Preparing keystore"
    rm -f "$keystore_path"
    create_keystore $keystore_path $CERTS_STORE_PASSWORD $certs_key_path/$tls_auth_cert $certs_key_path/$tls_auth_key $tls_auth_cert
    echo "Preparing keystore is complete"
fi

if [ -d /opt/strimzi/oauth-certs ]; then
  echo "Preparing truststore for OAuth"
  # Add each certificate to the trust store
  STORE=/tmp/strimzi/oauth.truststore.p12
  rm -f "$STORE"
  declare -i INDEX=0
  for CRT in /opt/strimzi/oauth-certs/**/*; do
    ALIAS="oauth-${INDEX}"
    echo "Adding $CRT to truststore $STORE with alias $ALIAS"
    create_truststore "$STORE" "$CERTS_STORE_PASSWORD" "$CRT" "$ALIAS"
    INDEX+=1
  done
  echo "Preparing truststore for OAuth is complete"
fi
