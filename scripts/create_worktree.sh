#!/usr/bin/env bash
set -euo pipefail

# -----------------------------------------------------------------------------
# Creates a git worktree rooted in ./worktrees/<worktree-name> for this repo.
# Usage: scripts/create_worktree.sh <worktree-name>
#   * Ensures the worktrees/ directory exists
#   * Creates a branch named <worktree-name> from the current HEAD when needed
#   * Adds the worktree bound to that branch
#   * Symlinks AGENTS.md and .codex from the repo root into the worktree
# -----------------------------------------------------------------------------

# Require exactly one positional argument for the worktree name.
if [[ $# -ne 1 ]]; then
  echo "Usage: $0 <worktree-name>" >&2
  exit 1
fi

WORKTREE_NAME="$1"

# Disallow names that could escape the worktrees directory or are otherwise unsafe.
if [[ "${WORKTREE_NAME}" =~ [/] ]] || [[ "${WORKTREE_NAME}" == "." ]] || [[ "${WORKTREE_NAME}" == ".." ]]; then
  echo "Invalid worktree name: ${WORKTREE_NAME}" >&2
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
WORKTREES_DIR="${REPO_ROOT}/worktrees"
WORKTREE_PATH="${WORKTREES_DIR}/${WORKTREE_NAME}"
BRANCH_NAME="${WORKTREE_NAME}"

# Ensure the parent directory for worktrees exists.
mkdir -p "${WORKTREES_DIR}"

# Reject collisions with non-git directories to avoid clobbering user data.
if [[ -d "${WORKTREE_PATH}" && ! -e "${WORKTREE_PATH}/.git" ]]; then
  echo "Directory ${WORKTREE_PATH} already exists and is not a git worktree" >&2
  exit 1
fi

# Create the branch if it does not exist, basing it on the current HEAD state.
if ! git -C "${REPO_ROOT}" show-ref --verify --quiet "refs/heads/${BRANCH_NAME}"; then
  git -C "${REPO_ROOT}" branch "${BRANCH_NAME}" HEAD
fi

list_worktree_paths() {
  # Prefer the modern --format flag when available, otherwise fall back to
  # parsing the porcelain output so older git versions still work.
  local output
  if output=$(git -C "${REPO_ROOT}" worktree list --format='%(path)' 2>/dev/null); then
    printf '%s\n' "${output}"
  else
    git -C "${REPO_ROOT}" worktree list --porcelain | awk '$1 == "worktree" {print $2}'
  fi
}

WORKTREE_EXISTS=0
while IFS= read -r path; do
  if [[ "${path}" == "${WORKTREE_PATH}" ]]; then
    WORKTREE_EXISTS=1
    break
  fi
done < <(list_worktree_paths)

if (( WORKTREE_EXISTS )); then
  # Refresh symlinks when the worktree already exists.
  echo "Worktree already exists at ${WORKTREE_PATH}; refreshing symlinks" >&2
else
  # Create the new worktree targeting the requested branch.
  git -C "${REPO_ROOT}" worktree add "${WORKTREE_PATH}" "${BRANCH_NAME}"
fi

for file in AGENTS.md .codex; do
  TARGET="${WORKTREE_PATH}/${file}"

  # Remove any existing file or stale link before creating the new symlink.
  if [[ -e "${TARGET}" || -L "${TARGET}" ]]; then
    rm -f "${TARGET}"
  fi

  # Use a stable relative path so links remain valid if the repository moves.
  SOURCE_REL="../../${file}"
  if [[ -e "${REPO_ROOT}/${file}" || -L "${REPO_ROOT}/${file}" ]]; then
    ln -s "${SOURCE_REL}" "${TARGET}"
  else
    # Warn when the source file is missing but continue so the script remains idempotent.
    echo "Warning: ${REPO_ROOT}/${file} not found; skipping symlink" >&2
  fi
done
