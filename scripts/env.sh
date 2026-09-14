# shellcheck shell=bash
# Source from the repository root:  source scripts/env.sh
# Optional overrides: MONACGRAPH_HOME, MONACGRAPH_CACHE, MONACGRAPH_KEEP_SYSTEM_TMP=1

_monac_scripts="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export MONACGRAPH_HOME="${MONACGRAPH_HOME:-$(cd "$_monac_scripts/.." && pwd)}"
export MONACGRAPH_CACHE="${MONACGRAPH_CACHE:-$MONACGRAPH_HOME/.cache}"
mkdir -p "$MONACGRAPH_CACHE/tmp" "$MONACGRAPH_CACHE/huggingface" "$MONACGRAPH_CACHE/pip"
export HF_HOME="${HF_HOME:-$MONACGRAPH_CACHE/huggingface}"
export HUGGINGFACE_HUB_CACHE="${HUGGINGFACE_HUB_CACHE:-$HF_HOME/hub}"
export TRANSFORMERS_CACHE="${TRANSFORMERS_CACHE:-$HF_HOME}"
export PIP_CACHE_DIR="${PIP_CACHE_DIR:-$MONACGRAPH_CACHE/pip}"

if [[ "${MONACGRAPH_KEEP_SYSTEM_TMP:-}" != "1" ]]; then
  export TMP="$MONACGRAPH_CACHE/tmp"
  export TEMP="$MONACGRAPH_CACHE/tmp"
  export TMPDIR="$MONACGRAPH_CACHE/tmp"
  _java_tmp="-Djava.io.tmpdir=$MONACGRAPH_CACHE/tmp"
  if [[ -z "${JAVA_TOOL_OPTIONS:-}" ]]; then
    export JAVA_TOOL_OPTIONS="$_java_tmp"
  elif [[ "$JAVA_TOOL_OPTIONS" != *"java.io.tmpdir"* ]]; then
    export JAVA_TOOL_OPTIONS="$JAVA_TOOL_OPTIONS $_java_tmp"
  fi
  unset _java_tmp
fi
unset _monac_scripts
