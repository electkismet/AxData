#!/usr/bin/env bash
set -euo pipefail

python_bin="${PYTHON:-python3}"
skip_npm=0

usage() {
  cat <<'EOF'
Usage: bash scripts/bootstrap.sh [--python PYTHON] [--skip-npm]

Prepare the AxData workspace on macOS/Linux:
  - create .venv
  - install local Python packages in editable mode
  - install Web dependencies unless --skip-npm is used
EOF
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --python)
      if [[ $# -lt 2 ]]; then
        echo "Missing value for --python" >&2
        exit 2
      fi
      python_bin="$2"
      shift 2
      ;;
    --skip-npm)
      skip_npm=1
      shift
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      echo "Unknown argument: $1" >&2
      usage >&2
      exit 2
      ;;
  esac
done

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
repo_root="$(cd "$script_dir/.." && pwd)"
venv_dir="$repo_root/.venv"
venv_python="$venv_dir/bin/python"

cd "$repo_root"

if [[ ! -x "$venv_python" ]]; then
  echo "Creating Python virtual environment..."
  "$python_bin" -m venv "$venv_dir"
fi

echo "Installing Python packages..."
"$venv_python" -m pip install -U pip
"$venv_python" -m pip install -e ".[dev]"

editable_packages=(
  "libs/axdata_core"
  "packages/axdata-source-tdx"
  "packages/axdata-source-tdx-ext"
  "packages/axdata-source-tencent"
  "packages/axdata-source-cninfo"
  "packages/axdata-sdk"
)

for package_path in "${editable_packages[@]}"; do
  "$venv_python" -m pip install -e "$package_path"
done

# 采集器插件包(axdata-collector-*),含共享 runner 支撑包
for collector_dir in "$repo_root"/packages/axdata-collector-*/; do
  [[ -d "$collector_dir" ]] || continue
  "$venv_python" -m pip install -e "$collector_dir"
done

if [[ "$skip_npm" -eq 0 ]]; then
  if ! command -v npm >/dev/null 2>&1; then
    echo "npm was not found. Install Node.js first, or rerun with --skip-npm to skip Web dependencies." >&2
    exit 1
  fi
  echo "Installing Web dependencies..."
  npm install
fi

echo
echo "AxData workspace is ready."
echo "Start API: ./.venv/bin/python -m uvicorn apps.api.main:app --host 127.0.0.1 --port 8666 --reload"
echo "Start Web: npm run dev:web"
