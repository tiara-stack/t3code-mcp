#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: sync-from-tiara-stack.sh [--source PATH]

Copy the autonomous-development skill from a tiara-stack checkout while
keeping this repository's CI gate and local validator scripts.

The source checkout can also be supplied with TIARA_STACK_ROOT. If neither is
set, the script looks for /opt/data/tiara-stack and the matching checkout path
relative to this repository.
USAGE
}

die() {
  printf 'sync-from-tiara-stack: %s\n' "$*" >&2
  exit 1
}

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
repo_root=$(git -C "$script_dir" rev-parse --show-toplevel 2>/dev/null) ||
  die "run this script from a Git checkout"
destination_skill="$repo_root/.agents/skills/autonomous-development"

source_root=${TIARA_STACK_ROOT:-}
while (($# > 0)); do
  case "$1" in
    --help|-h)
      usage
      exit 0
      ;;
    --source)
      (($# >= 2)) || die "--source requires a path"
      source_root=$2
      shift 2
      ;;
    --source=*)
      source_root=${1#*=}
      shift
      ;;
    --)
      shift
      (($# == 0)) || die "unexpected argument: $1"
      ;;
    -* )
      die "unknown option: $1"
      ;;
    *)
      [[ -z "$source_root" ]] || die "source path supplied more than once"
      source_root=$1
      shift
      ;;
  esac
done

if [[ -z "$source_root" ]]; then
  for candidate in "$repo_root/../../../tiara-stack" "/opt/data/tiara-stack"; do
    if [[ -d "$candidate/.agents/skills/autonomous-development" ]]; then
      source_root=$candidate
      break
    fi
  done
fi

[[ -n "$source_root" ]] ||
  die "tiara-stack checkout not found; pass --source PATH or set TIARA_STACK_ROOT"
[[ -d "$source_root" ]] || die "source checkout does not exist: $source_root"
source_root=$(cd -- "$source_root" && pwd -P)
source_skill="$source_root/.agents/skills/autonomous-development"
[[ -d "$source_skill" ]] || die "source skill does not exist: $source_skill"
[[ -d "$destination_skill" ]] || die "destination skill does not exist: $destination_skill"

command -v rsync >/dev/null 2>&1 || die "rsync is required"
command -v perl >/dev/null 2>&1 || die "perl is required"

local_skill="$destination_skill/SKILL.md"
[[ -f "$local_skill" ]] || die "destination skill is missing SKILL.md"

local_table=$(awk '
  index($0, "| `pre-undraft <PR>` |") { print; exit }
' "$local_skill")
[[ -n "$local_table" ]] || die "could not find the pre-undraft route in the local SKILL.md"
if ! grep -Fq "repository's" <<<"$local_table" ||
  ! grep -Fq 'checks' <<<"$local_table"; then
  die "local SKILL.md does not contain this repository's checks gate"
fi

local_undraft_block=$(awk '
  /^`pre-undraft` completes only when/ { capture=1 }
  capture { print }
  capture && /^continues from that gate with:$/ { exit }
' "$local_skill")
[[ -n "$local_undraft_block" ]] ||
  die "could not find the local pre-undraft completion criterion"
if ! grep -Fq "repository's" <<<"$local_undraft_block" ||
  ! grep -Fq 'checks' <<<"$local_undraft_block"; then
  die "local SKILL.md does not contain the local CI completion criterion"
fi

staging_root=$(mktemp -d "${TMPDIR:-/tmp}/autonomous-development-sync.XXXXXX")
replacement_root=
backup_root=
destination_moved=0
swap_complete=0
rollback_failed=0
cleanup() {
  local status=$?

  if ((destination_moved == 1 && swap_complete == 0)) &&
    [[ ! -e "$destination_skill" && -e "$backup_root/skill" ]]; then
    if ! mv -- "$backup_root/skill" "$destination_skill"; then
      rollback_failed=1
      printf 'sync-from-tiara-stack: rollback failed; backup remains at %s\n' \
        "$backup_root/skill" >&2
    fi
  fi

  [[ -z "$staging_root" ]] || rm -rf -- "$staging_root" || true
  [[ -z "$replacement_root" ]] || rm -rf -- "$replacement_root" || true
  if ((rollback_failed == 0)); then
    [[ -z "$backup_root" ]] || rm -rf -- "$backup_root" || true
  fi
  exit "$status"
}
trap cleanup EXIT

# The CI references and scripts are maintained by this repository. Copy the
# rest of the source skill into a temporary tree so a failed merge cannot leave
# a partially updated skill behind.
rsync -a \
  --exclude='/references/ci-gates.md' \
  --exclude='/references/ci-polling.md' \
  --exclude='/scripts/***' \
  "$source_skill/" "$staging_root/"

for local_path in \
  references/ci-gates.md \
  references/ci-polling.md \
  scripts; do
  [[ -e "$destination_skill/$local_path" ]] ||
    die "local CI override is missing: $destination_skill/$local_path"
  mkdir -p -- "$staging_root/$(dirname -- "$local_path")"
  cp -a -- "$destination_skill/$local_path" "$staging_root/$(dirname -- "$local_path")/"
done

replace_exact() {
  local file=$1
  local old_text=$2
  local new_text=$3
  local match_count

  match_count=$(OLD_TEXT="$old_text" perl -0ne '
    my $count = () = /\Q$ENV{OLD_TEXT}\E/g;
    print $count;
  ' "$file")
  [[ "$match_count" == 1 ]] ||
    die "expected one upstream CI block in $file, found $match_count"

  OLD_TEXT="$old_text" NEW_TEXT="$new_text" perl -0pi -e \
    's/\Q$ENV{OLD_TEXT}\E/$ENV{NEW_TEXT}/s' "$file"
}

staged_skill="$staging_root/SKILL.md"
[[ -f "$staged_skill" ]] || die "source skill is missing SKILL.md"

expected_upstream_table='| `pre-undraft <PR>` | Babysit CI and repair the PR | Named CI checks are green; leave the PR draft state unchanged |'
expected_upstream_undraft_block=$(cat <<'EOF'
`pre-undraft` completes only when the named checks and every required check are
green for the current head. It stops before changing draft state. `undraft`
continues from that gate with:
EOF
)

upstream_table=$(awk '
  index($0, "| `pre-undraft <PR>` |") { print; exit }
' "$staged_skill")
if [[ "$upstream_table" == "$expected_upstream_table" ]]; then
  replace_exact "$staged_skill" "$upstream_table" "$local_table"
elif [[ "$upstream_table" != "$local_table" ]]; then
  die "upstream SKILL.md changed its pre-undraft route; update the local gate override"
fi

upstream_undraft_block=$(awk '
  /^`pre-undraft` completes only when/ { capture=1 }
  capture { print }
  capture && /^continues from that gate with:$/ { exit }
' "$staged_skill")
if [[ "$upstream_undraft_block" == "$expected_upstream_undraft_block" ]]; then
  replace_exact "$staged_skill" "$upstream_undraft_block" "$local_undraft_block"
elif [[ "$upstream_undraft_block" != "$local_undraft_block" ]]; then
  die "upstream SKILL.md changed its CI completion criterion; update the local gate override"
fi

skill_parent=$(dirname -- "$destination_skill")
replacement_root=$(mktemp -d "$skill_parent/.autonomous-development-sync.XXXXXX")
cp -a -- "$destination_skill/." "$replacement_root/"
rsync -a \
  --delete \
  --exclude='/references/ci-gates.md' \
  --exclude='/references/ci-polling.md' \
  --exclude='/scripts/***' \
  "$staging_root/" "$replacement_root/"

[[ -f "$replacement_root/SKILL.md" ]] || die "replacement skill is missing SKILL.md"
[[ -f "$replacement_root/references/ci-gates.md" ]] ||
  die "replacement skill is missing the local CI gate reference"
[[ -f "$replacement_root/references/ci-polling.md" ]] ||
  die "replacement skill is missing the local CI polling reference"
[[ -f "$replacement_root/scripts/required-checks-pass.sh" ]] ||
  die "replacement skill is missing the local required-check validator"
find "$replacement_root" -type f -name '*.sh' -exec bash -n {} +

backup_root=$(mktemp -d "$skill_parent/.autonomous-development-backup.XXXXXX")
mv -- "$destination_skill" "$backup_root/skill"
destination_moved=1
mv -- "$replacement_root" "$destination_skill"
swap_complete=1

printf 'Synced autonomous-development skill from %s\n' "$source_root"
printf '%s\n' "Kept this repository's checks gate and local CI validator scripts."
