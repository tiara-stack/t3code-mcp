#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd -P)
skill_dir=$(cd -- "$script_dir/.." && pwd -P)
test_root=$(mktemp -d "${TMPDIR:-/tmp}/autonomous-development-sync-test.XXXXXX")
trap 'rm -rf -- "$test_root"' EXIT

source_root="$test_root/tiara-stack"
destination_root="$test_root/repo"
mkdir -p -- "$source_root/.agents/skills" "$destination_root/.agents/skills"
cp -a -- "$skill_dir" "$destination_root/.agents/skills/autonomous-development"
cp -a -- "$skill_dir" "$source_root/.agents/skills/autonomous-development"
git -C "$destination_root" init -q

source_skill="$source_root/.agents/skills/autonomous-development"
source_table=$(awk '
  index($0, "| `pre-undraft <PR>` |") { print; exit }
' "$source_skill/SKILL.md")
source_block=$(awk '
  /^`pre-undraft` completes only when/ { capture=1 }
  capture { print }
  capture && /^continues from that gate with:$/ { exit }
' "$source_skill/SKILL.md")
upstream_table='| `pre-undraft <PR>` | Babysit CI and repair the PR | Named CI checks are green; leave the PR draft state unchanged |'
upstream_block=$(cat <<'EOF'
`pre-undraft` completes only when the named checks and every required check are
green for the current head. It stops before changing draft state. `undraft`
continues from that gate with:
EOF
)
OLD_TEXT="$source_table" NEW_TEXT="$upstream_table" perl -0pi -e \
  's/\Q$ENV{OLD_TEXT}\E/$ENV{NEW_TEXT}/s' "$source_skill/SKILL.md"
OLD_TEXT="$source_block" NEW_TEXT="$upstream_block" perl -0pi -e \
  's/\Q$ENV{OLD_TEXT}\E/$ENV{NEW_TEXT}/s' "$source_skill/SKILL.md"
printf '\nSource sync test marker.\n' >>"$source_skill/SKILL.md"
printf '%s\n' 'source-only file' >"$source_skill/references/source-only.md"
printf '%s\n' 'destination-only file' >"$destination_root/.agents/skills/autonomous-development/references/destination-only.md"

bash "$destination_root/.agents/skills/autonomous-development/scripts/sync-from-tiara-stack.sh" \
  --source "$source_root"

destination_skill="$destination_root/.agents/skills/autonomous-development"
grep -Fq 'Source sync test marker.' "$destination_skill/SKILL.md"
grep -Fq "repository's" "$destination_skill/SKILL.md"
grep -Fq 'checks' "$destination_skill/SKILL.md"
if grep -Fq 'Named CI checks are green' "$destination_skill/SKILL.md"; then
  printf '%s\n' 'local checks gate was overwritten' >&2
  exit 1
fi

test -f "$destination_skill/references/source-only.md"
test ! -e "$destination_skill/references/destination-only.md"
cmp "$skill_dir/references/ci-gates.md" "$destination_skill/references/ci-gates.md"
cmp "$skill_dir/references/ci-polling.md" "$destination_skill/references/ci-polling.md"
cmp "$skill_dir/scripts/required-checks-pass.sh" \
  "$destination_skill/scripts/required-checks-pass.sh"

expect_rejected_source() {
  local name=$1
  local near_source_root=$2
  local near_destination_root=$3

  if bash "$near_destination_root/.agents/skills/autonomous-development/scripts/sync-from-tiara-stack.sh" \
    --source "$near_source_root" >/dev/null 2>&1; then
    printf 'near-match source was accepted: %s\n' "$name" >&2
    exit 1
  fi
  cmp "$skill_dir/SKILL.md" \
    "$near_destination_root/.agents/skills/autonomous-development/SKILL.md"
}

near_table_source_root="$test_root/near-table-source"
near_table_destination_root="$test_root/near-table-destination"
mkdir -p -- "$near_table_source_root/.agents/skills" \
  "$near_table_destination_root/.agents/skills"
cp -a -- "$source_root/.agents/skills/autonomous-development" \
  "$near_table_source_root/.agents/skills/autonomous-development"
cp -a -- "$skill_dir" \
  "$near_table_destination_root/.agents/skills/autonomous-development"
git -C "$near_table_destination_root" init -q
near_table_skill="$near_table_source_root/.agents/skills/autonomous-development/SKILL.md"
near_table_source=$(awk '
  index($0, "| `pre-undraft <PR>` |") { print; exit }
' "$near_table_skill")
near_table_replacement='| `pre-undraft <PR>` | Babysit CI and repair the PR | Named CI checks are green for this checkout; leave the PR draft state unchanged |'
OLD_TEXT="$near_table_source" NEW_TEXT="$near_table_replacement" perl -0pi -e \
  's/\Q$ENV{OLD_TEXT}\E/$ENV{NEW_TEXT}/s' "$near_table_skill"
expect_rejected_source 'pre-undraft route' "$near_table_source_root" "$near_table_destination_root"

near_block_source_root="$test_root/near-block-source"
near_block_destination_root="$test_root/near-block-destination"
mkdir -p -- "$near_block_source_root/.agents/skills" \
  "$near_block_destination_root/.agents/skills"
cp -a -- "$source_root/.agents/skills/autonomous-development" \
  "$near_block_source_root/.agents/skills/autonomous-development"
cp -a -- "$skill_dir" \
  "$near_block_destination_root/.agents/skills/autonomous-development"
git -C "$near_block_destination_root" init -q
near_block_skill="$near_block_source_root/.agents/skills/autonomous-development/SKILL.md"
near_block_source=$(awk '
  /^`pre-undraft` completes only when/ { capture=1 }
  capture { print }
  capture && /^continues from that gate with:$/ { exit }
' "$near_block_skill")
near_block_replacement=$(printf '%s\n' "$near_block_source" | sed 's/named checks/configured checks/')
OLD_TEXT="$near_block_source" NEW_TEXT="$near_block_replacement" perl -0pi -e \
  's/\Q$ENV{OLD_TEXT}\E/$ENV{NEW_TEXT}/s' "$near_block_skill"
expect_rejected_source 'pre-undraft completion criterion' \
  "$near_block_source_root" "$near_block_destination_root"

failure_root="$test_root/failure-repo"
mkdir -p -- "$failure_root/.agents/skills"
cp -a -- "$skill_dir" "$failure_root/.agents/skills/autonomous-development"
git -C "$failure_root" init -q

mv_stub_dir="$test_root/mv-stub"
mkdir -p -- "$mv_stub_dir"
real_mv=$(command -v mv)
cat >"$mv_stub_dir/mv" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail

count=0
if [[ -f "$MV_COUNT" ]]; then
  count=$(<"$MV_COUNT")
fi
count=$((count + 1))
printf '%s\n' "$count" >"$MV_COUNT"
if [[ "$count" == 2 ]]; then
  exit 75
fi
exec "$REAL_MV" "$@"
STUB
chmod +x "$mv_stub_dir/mv"

set +e
PATH="$mv_stub_dir:$PATH" MV_COUNT="$test_root/mv-count" REAL_MV="$real_mv" \
  bash "$failure_root/.agents/skills/autonomous-development/scripts/sync-from-tiara-stack.sh" \
    --source "$source_root"
sync_status=$?
set -e
if [[ "$sync_status" == 0 ]]; then
  printf '%s\n' 'interrupted final update unexpectedly succeeded' >&2
  exit 1
fi

failure_skill="$failure_root/.agents/skills/autonomous-development"
cmp "$skill_dir/SKILL.md" "$failure_skill/SKILL.md"
test ! -e "$failure_skill/references/source-only.md"
test -f "$failure_skill/scripts/sync-from-tiara-stack.sh"
if find "$failure_root/.agents/skills" -maxdepth 1 -type d \
  \( -name '.autonomous-development-sync.*' -o -name '.autonomous-development-backup.*' \) \
  -print -quit | grep -q .; then
  printf '%s\n' 'temporary replacement or backup was left behind' >&2
  exit 1
fi

printf '%s\n' 'Autonomous-development sync: passed'
