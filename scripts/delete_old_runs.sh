#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Delete GitHub Actions workflow runs older than a given age.

Usage:
  delete_old_runs.sh --repo owner/name --older-than 30d [--workflow WORKFLOW] [--limit N] [--dry-run]

Options:
  --repo          Repository in owner/name form.
  --older-than    Age threshold (e.g. 30d, 12h, 90m, 1hour, 30minutes).
  --workflow      Optional workflow name or ID to filter.
  --limit         Max runs to delete (default: 1000).
  --dry-run       Show runs that would be deleted, but do not delete.

Requires:
  gh (GitHub CLI) authenticated with access to the repo.
EOF
}

repo=""
older_than=""
workflow=""
limit=1000
dry_run=0

while [[ $# -gt 0 ]]; do
  case "$1" in
    --repo)
      repo="${2:-}"
      shift 2
      ;;
    --older-than)
      older_than="${2:-}"
      shift 2
      ;;
    --workflow)
      workflow="${2:-}"
      shift 2
      ;;
    --limit)
      limit="${2:-}"
      shift 2
      ;;
    --dry-run)
      dry_run=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage
      exit 1
      ;;
  esac
done

if [[ -z "$repo" || -z "$older_than" ]]; then
  usage
  exit 1
fi

if ! command -v gh >/dev/null 2>&1; then
  echo "gh is required but not found in PATH." >&2
  exit 1
fi

list_args=(run list --repo "$repo" --json databaseId,createdAt,displayTitle,workflowName)

if [[ -n "$workflow" ]]; then
  list_args+=(--workflow "$workflow")
fi

if [[ "$limit" -gt 0 ]]; then
  list_args+=(--limit "$limit")
fi

cutoff_epoch="$(
  python3 - <<'PY' "$older_than" 2>/dev/null || python - <<'PY' "$older_than" 2>/dev/null || true
import sys
import time

arg = sys.argv[1].strip().lower()
units = {
    "d": 86400,
    "day": 86400,
    "days": 86400,
    "h": 3600,
    "hr": 3600,
    "hrs": 3600,
    "hour": 3600,
    "hours": 3600,
    "m": 60,
    "min": 60,
    "mins": 60,
    "minute": 60,
    "minutes": 60,
}

value_part = ""
unit_part = ""
for i, ch in enumerate(arg):
    if ch.isdigit():
        value_part += ch
    else:
        unit_part = arg[i:]
        break

if not value_part or not unit_part or unit_part not in units:
    sys.exit(1)

value = int(value_part)
mult = units[unit_part]
cutoff = int(time.time()) - (value * mult)
print(cutoff)
PY
)"
if [[ -z "$cutoff_epoch" ]]; then
  echo "Invalid --older-than value: $older_than (expected Nd/Nh/Nm)" >&2
  exit 1
fi

to_delete=()
candidates=()

if ! gh_output="$(
  gh "${list_args[@]}" \
    --jq '.[] | select(.createdAt != null) |
          [(.createdAt | fromdateiso8601), .databaseId, .workflowName, .displayTitle] |
          @tsv'
)"; then
  echo "Failed to fetch runs from GitHub. Check network access and gh auth." >&2
  exit 1
fi

while IFS= read -r line; do
  [[ -z "$line" ]] && continue
  candidates+=("$line")
done <<<"$gh_output"
for row in "${candidates[@]}"; do
  created_epoch="$(cut -f1 <<<"$row")"
  run_id="$(cut -f2 <<<"$row")"
  wf_name="$(cut -f3 <<<"$row")"
  title="$(cut -f4 <<<"$row")"

  if [[ "$created_epoch" -lt "$cutoff_epoch" ]]; then
    to_delete+=("$run_id	$wf_name	$title")
  fi
done

if [[ ${#to_delete[@]} -eq 0 ]]; then
  echo "No runs older than $older_than found for $repo."
  exit 0
fi

if [[ "$dry_run" -eq 1 ]]; then
  echo "Dry run: would delete ${#to_delete[@]} runs older than $older_than."
  printf '%s\n' "${to_delete[@]}"
  exit 0
fi

echo "Deleting ${#to_delete[@]} runs older than $older_than from $repo..."
delete_args=(--repo "$repo")
help_text="$(gh run delete -h 2>&1)"
if command -v rg >/dev/null 2>&1; then
  if rg -q -- '--yes' <<<"$help_text"; then
    delete_args+=(--yes)
  elif rg -q -- '--confirm' <<<"$help_text"; then
    delete_args+=(--confirm)
  fi
else
  if grep -q -- '--yes' <<<"$help_text"; then
    delete_args+=(--yes)
  elif grep -q -- '--confirm' <<<"$help_text"; then
    delete_args+=(--confirm)
  fi
fi

for entry in "${to_delete[@]}"; do
  run_id="$(cut -f1 <<<"$entry")"
  gh run delete "$run_id" "${delete_args[@]}"
done

echo "Done."
