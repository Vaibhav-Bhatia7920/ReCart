#!/bin/bash
# First-time TLS bootstrap for voice-api.vabhatia.dev on the GCP VPS.
# Run from the directory that contains docker-compose.yml (e.g. cart-recovery/ReCart).
# Let's Encrypt material is stored in ../certbot by default (sibling of ReCart).
set -euo pipefail

DOMAIN="${DOMAIN:-voice-api.vabhatia.dev}"
EMAIL="${CERTBOT_EMAIL:?Set CERTBOT_EMAIL to a real address for Let's Encrypt notices}"
RSA_KEY_SIZE="${RSA_KEY_SIZE:-4096}"
STAGING="${STAGING:-0}"  # set STAGING=1 to dry-run against Let's Encrypt staging

COMPOSE="docker compose"
compose_dir="$(cd "$(dirname "$0")/.." && pwd)"
data_path="${CERTBOT_DATA_PATH:-../certbot}"
certbot_root="$(cd "$compose_dir/$data_path" && pwd)"
conf_mount="$certbot_root/conf"

CERTBOT_TAG="${CERTBOT_TAG:-v4.0.0}"
OPTIONS_SSL_URL="https://raw.githubusercontent.com/certbot/certbot/${CERTBOT_TAG}/certbot-nginx/certbot_nginx/_internal/tls_configs/options-ssl-nginx.conf"
DHPARAMS_URL="https://raw.githubusercontent.com/certbot/certbot/${CERTBOT_TAG}/certbot/certbot/ssl-dhparams.pem"

if ! command -v docker >/dev/null 2>&1; then
  echo "docker is required" >&2
  exit 1
fi

if [ ! -e "$conf_mount/options-ssl-nginx.conf" ] || [ ! -e "$conf_mount/ssl-dhparams.pem" ]; then
  echo "### Downloading recommended TLS parameters ..."
  mkdir -p "$conf_mount"
  curl -fsSL "$OPTIONS_SSL_URL" -o "$conf_mount/options-ssl-nginx.conf"
  curl -fsSL "$DHPARAMS_URL" -o "$conf_mount/ssl-dhparams.pem"
fi

echo "### Creating dummy certificate for $DOMAIN ..."
mkdir -p "$conf_mount/live/$DOMAIN"
docker run --rm -v "$conf_mount:/etc/letsencrypt" alpine sh -c "\
  apk add --no-cache openssl >/dev/null && \
  openssl req -x509 -nodes -newkey rsa:2048 -days 1 \
    -keyout '/etc/letsencrypt/live/$DOMAIN/privkey.pem' \
    -out '/etc/letsencrypt/live/$DOMAIN/fullchain.pem' \
    -subj '/CN=localhost'"

echo "### Starting nginx ..."
cd "$compose_dir"
$COMPOSE up -d nginx

echo "### Deleting dummy certificate ..."
docker run --rm -v "$conf_mount:/etc/letsencrypt" alpine \
  rm -rf "/etc/letsencrypt/live/$DOMAIN" "/etc/letsencrypt/archive/$DOMAIN" "/etc/letsencrypt/renewal/$DOMAIN.conf"

echo "### Requesting Let's Encrypt certificate for $DOMAIN ..."
staging_arg=""
if [ "$STAGING" != "0" ]; then
  staging_arg="--staging"
fi

$COMPOSE run --rm --entrypoint certbot certbot certonly \
  --webroot -w /var/www/certbot \
  $staging_arg \
  --email "$EMAIL" \
  -d "$DOMAIN" \
  --rsa-key-size "$RSA_KEY_SIZE" \
  --agree-tos \
  --force-renewal \
  --non-interactive

echo "### Reloading nginx ..."
$COMPOSE exec nginx nginx -s reload

echo "Done. https://$DOMAIN/health should respond when the full stack is up:"
echo "  $COMPOSE up -d"
