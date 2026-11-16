# $0 is the stage script, $1 the task id, $2 the repository, $3 the command ID.
# Success means the worker sourced its preamble and acquired its pod lock.
set -eu
[ -x "$0" ] || { echo "stage is not executable" >&2; exit 1; }
grep -qx '# dispatcher-command-protocol: 2' "$0" || { echo "worker does not support dispatcher commands" >&2; exit 1; }
command -v setsid >/dev/null
command -v nohup >/dev/null
dir=$(mktemp -d /tmp/kaneo-launch.XXXXXX)
trap 'rm -rf "$dir"' EXIT
NIGHTLY_LAUNCH_ACK="$dir/ack" setsid --wait nohup "$0" "$1" "$2" "${3:-}" >/dev/null 2>&1 </dev/null &
pid=$!
check_ack() {
  [ -f "$dir/ack" ] || return 0
  case "$(cat "$dir/ack")" in
    accepted) exit 0 ;;
    busy) exit 75 ;;
    *) echo "invalid worker acknowledgment" >&2; exit 1 ;;
  esac
}
i=0
while [ "$i" -lt 200 ]; do
  check_ack
  if ! kill -0 "$pid" 2>/dev/null; then
    # The worker may acknowledge and exit between the two checks.
    wait "$pid" || true
    check_ack
    echo "worker exited without acknowledging launch" >&2
    exit 1
  fi
  sleep 0.1
  i=$((i + 1))
done
echo "worker did not acknowledge launch within 20 seconds" >&2
exit 1
