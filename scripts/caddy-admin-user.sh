#!/usr/bin/env bash
#
# Add (or replace) an HTTP basic-auth credential for the admin UIs - the Flink
# dashboard, Kafka UI, InfluxDB and the MinIO console, all fronted by Caddy.
#
#   ./scripts/caddy-admin-user.sh alice
#
# Each user gets their own file. To revoke access, delete one file:
#
#   rm caddy/credentials/10-alice.txt && docker compose restart caddy
#
set -euo pipefail

CRED_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)/caddy/credentials"

usage() {
	echo "usage: $0 <username>" >&2
	echo "  e.g. $0 alice" >&2
	exit 64
}

[ $# -eq 1 ] || usage
USERNAME="$1"

case "$USERNAME" in
	*[!A-Za-z0-9._-]* | "")
		echo "error: username must be non-empty and only contain A-Z a-z 0-9 . _ -" >&2
		exit 65
		;;
esac

if [ "$USERNAME" = "locked-placeholder" ]; then
	echo "error: 'locked-placeholder' is reserved - it is the fail-closed default" >&2
	exit 65
fi

command -v docker >/dev/null || { echo "error: docker not found" >&2; exit 69; }
mkdir -p "$CRED_DIR"

OUT="$CRED_DIR/10-$USERNAME.txt"
if [ -e "$OUT" ]; then
	printf 'Credential for %s already exists. Replace it? [y/N] ' "$USERNAME"
	read -r reply
	case "$reply" in [yY]*) ;; *) echo "aborted"; exit 0 ;; esac
fi

echo "Creating an admin credential for '$USERNAME'."
echo "Type the password at the prompt - it will not be echoed."

HASH="$(docker compose run --rm -it --no-deps --entrypoint caddy caddy \
	hash-password --algorithm bcrypt 2>/dev/null | tr -d '\r' | tail -1)"

case "$HASH" in
	'$2a$'* | '$2b$'* | '$2y$'*) ;;
	*)
		echo "error: did not get a hash back from caddy (got: ${HASH:0:40})" >&2
		exit 70
		;;
esac

# Written before the file is readable
UMASK_OLD=$(umask)
umask 077
printf '%s %s\n' "$USERNAME" "$HASH" > "$OUT"
umask "$UMASK_OLD"

echo
echo "Wrote $OUT"
echo "Apply it with:"
echo "    docker compose restart caddy"
