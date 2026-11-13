#!/bin/sh
set -eu

/snap/bin/certbot renew \
  --quiet \
  --cert-name 198.51.100.10 \
  --deploy-hook "/bin/systemctl restart caddy"

exec /opt/w/apps/w-papers-doi-catalog/deploy/verify-ip-certificate.sh
