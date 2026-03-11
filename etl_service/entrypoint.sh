#!/usr/bin/env sh
set -eu

CRON_SCHEDULE="${CRON_SCHEDULE:-*/1 * * * *}"

cat >/usr/local/bin/run_replicator.sh <<'EOF'
#!/usr/bin/env sh
set -eu
cd /app
python -m replicator.main
EOF
chmod +x /usr/local/bin/run_replicator.sh

cat >/etc/cron.d/replicator <<EOF
SHELL=/bin/sh
PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin

${CRON_SCHEDULE} root /usr/local/bin/run_replicator.sh >>/proc/1/fd/1 2>>/proc/1/fd/2
EOF
chmod 0644 /etc/cron.d/replicator

/usr/local/bin/run_replicator.sh >>/proc/1/fd/1 2>>/proc/1/fd/2 || true

exec cron -f

