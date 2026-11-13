#!/bin/sh
set -eu

certificate=/etc/letsencrypt/live/198.51.100.10/fullchain.pem

# A six-day certificate should never approach the final two days unnoticed.
/usr/bin/openssl x509 -checkend 172800 -noout -in "$certificate"
/usr/bin/curl --fail --silent --show-error --max-time 15 \
  https://198.51.100.10/health >/dev/null
