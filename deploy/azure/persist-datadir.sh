#!/usr/bin/env bash
# Data-directory persistence for scale-to-zero hosting.
#
# Azure Container Apps stops the container 30 minutes after the last request and
# starts a fresh one on the next visit, so the container's own disk is empty
# every time it wakes. The engine keeps working on that fast local disk, and this
# wrapper copies the data directory to the Azure Files share mounted at
# PERSIST_DIR:
#
#   on start     restore the newest readable archive
#   every 5 min  archive again if anything changed
#   on stop      archive once more after the server has flushed and exited
#
# A periodic archive is taken with the server paused (SIGSTOP), so the copy is
# exactly what a crash at that instant would leave on disk, and the engine is
# built to recover from that: the WAL tail is repaired on open, and SSTables and
# the manifest are written to `.pending` files and renamed into place. The pause
# lasts as long as a local `tar` of the directory, well under a second for a
# demo-sized dataset.
#
# The showcase bucket is never archived. The server re-seeds an empty showcase
# on every start, which keeps its incident 90 minutes before "now" instead of
# days in the past.
#
# Without PERSIST_DIR the wrapper simply runs the server, so `docker run`,
# Compose and Render behave exactly as before.
set -uo pipefail

persist_dir="${PERSIST_DIR:-}"
if [ -z "$persist_dir" ]; then
  exec "$@"
fi

data_dir="${FLUXDB_DATA_DIR:-/app/data}"
interval="${PERSIST_INTERVAL_SECONDS:-300}"
stop_timeout="${PERSIST_STOP_TIMEOUT_SECONDS:-90}"
archive="$persist_dir/fluxdb-data.tar.gz"
scratch="${TMPDIR:-/tmp}/fluxdb-data"
# Engine database of the shared showcase: t{DEMO_PROJECT_ID}_{DEMO_BUCKET}.
showcase="tdemofluxdb99_production"

log() { echo "[persist] $*"; }

restore() {
  local candidate
  for candidate in "$archive" "$archive.prev"; do
    [ -s "$candidate" ] || continue
    rm -rf "$data_dir.restoring"
    mkdir -p "$data_dir.restoring"
    if tar -xzf "$candidate" -C "$data_dir.restoring"; then
      rm -rf "$data_dir"
      mv "$data_dir.restoring" "$data_dir"
      restored_from="$candidate"
      log "restored $data_dir from $candidate"
      return 0
    fi
    log "skipping unreadable archive $candidate"
  done
  rm -rf "$data_dir.restoring"
  mkdir -p "$data_dir"
  log "no archive in $persist_dir yet; starting with an empty data directory"
  return 1
}

# Size and modification time of every file an archive would contain.
fingerprint() {
  find "$data_dir" \( -path "$data_dir/$showcase" -o -name '.deleted-*' \) -prune -o \
    -type f ! -name '.lock' ! -name '*.pending' -printf '%T@ %s %p\n' 2>/dev/null |
    sort | md5sum
}

# Pass "pause" to freeze the running server while its files are copied.
save() {
  local paused=0 copied=0
  if [ "${1:-}" = "pause" ] && kill -STOP "$app_pid" 2>/dev/null; then paused=1; fi
  tar -cf "$scratch.tar" -C "$data_dir" \
    --exclude="./$showcase" --exclude='.lock' --exclude='*.pending' --exclude='./.deleted-*' . &&
    copied=1
  if [ "$paused" -eq 1 ]; then kill -CONT "$app_pid" 2>/dev/null; fi
  if [ "$copied" -ne 1 ]; then
    log "archive failed: could not read $data_dir"
    rm -f "$scratch.tar"
    return 1
  fi
  # Compress and upload after the server is running again, under a temporary
  # name, so an interrupted upload never replaces a good archive.
  if gzip -c "$scratch.tar" > "$scratch.tar.gz" && cp "$scratch.tar.gz" "$archive.partial"; then
    if [ -f "$archive" ]; then mv -f "$archive" "$archive.prev"; fi
    mv -f "$archive.partial" "$archive"
    rm -f "$scratch.tar" "$scratch.tar.gz"
    log "saved $(stat -c %s "$archive") bytes to $archive"
    return 0
  fi
  log "archive failed: could not write to $persist_dir"
  rm -f "$archive.partial" "$scratch.tar" "$scratch.tar.gz"
  return 1
}

stopping=0
app_pid=
sleep_pid=
stop_deadline=0
request_stop() {
  stopping=1
  stop_deadline=$((SECONDS + stop_timeout))
  log "stop requested; waiting for the server to flush and exit"
  if [ -n "$app_pid" ]; then kill -TERM "$app_pid" 2>/dev/null; fi
  if [ -n "$sleep_pid" ]; then kill "$sleep_pid" 2>/dev/null; fi
}
trap request_stop TERM INT

# Wait until the server exits, killing it if it outlives a stop request, and
# leave its exit status in $status.
wait_for_server() {
  while kill -0 "$app_pid" 2>/dev/null; do
    if [ "$stopping" -eq 1 ] && [ "$SECONDS" -ge "$stop_deadline" ]; then
      log "server did not exit within ${stop_timeout}s; killing it"
      kill -KILL "$app_pid" 2>/dev/null
    fi
    sleep 1
  done
  wait "$app_pid"
  status=$?
}

restored=0
restored_from=
if restore; then restored=1; fi
rm -rf "${data_dir:?}/$showcase"

fallback=0
while :; do
  "$@" &
  app_pid=$!
  started=$SECONDS
  last="$(fingerprint)"
  next_save=$((SECONDS + interval))
  # Short sleeps so a crashed server is noticed within seconds.
  while [ "$stopping" -eq 0 ] && kill -0 "$app_pid" 2>/dev/null; do
    sleep 5 &
    sleep_pid=$!
    wait "$sleep_pid" 2>/dev/null
    sleep_pid=
    [ "$stopping" -eq 0 ] && [ "$SECONDS" -ge "$next_save" ] || continue
    next_save=$((SECONDS + interval))
    current="$(fingerprint)"
    if [ "$current" != "$last" ] && save pause; then
      last="$current"
    fi
  done
  wait_for_server

  # A server that dies within seconds of opening restored data may be unable to
  # read it. Keep a copy of the archive and try once with an empty data
  # directory, so a bad archive cannot keep the demo down. If an empty start
  # fails as well, the archive was not the problem: drop the copy and give up.
  early=$((SECONDS - started < 30))
  if [ "$stopping" -eq 0 ] && [ "$status" -ne 0 ] && [ "$early" -eq 1 ]; then
    if [ "$restored" -eq 1 ] && [ "$fallback" -eq 0 ]; then
      log "server exited ($status) right after restoring; keeping $archive.quarantined and starting empty"
      cp -f "$restored_from" "$archive.quarantined" 2>/dev/null
      rm -rf "$data_dir"
      mkdir -p "$data_dir"
      fallback=1
      continue
    fi
    if [ "$fallback" -eq 1 ]; then rm -f "$archive.quarantined"; fi
    exit "$status"
  fi
  break
done

# The shutdown flush usually changes something; an untouched directory is
# already in the archive.
if [ "$(fingerprint)" != "$last" ]; then
  save
fi
exit "$status"
