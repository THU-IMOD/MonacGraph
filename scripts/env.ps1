# Dot-source from the repository root:  . .\scripts\env.ps1
# Optional overrides: MONACGRAPH_HOME, MONACGRAPH_CACHE, MONACGRAPH_KEEP_SYSTEM_TMP=1

$ErrorActionPreference = "Stop"
if (-not $PSScriptRoot) {
    throw "Run this file with: . .\scripts\env.ps1"
}

$Root = (Resolve-Path (Join-Path $PSScriptRoot "..")).Path
if (-not $env:MONACGRAPH_HOME) {
    $env:MONACGRAPH_HOME = $Root
}
if (-not $env:MONACGRAPH_CACHE) {
    $env:MONACGRAPH_CACHE = Join-Path $env:MONACGRAPH_HOME ".cache"
}

New-Item -ItemType Directory -Force -Path (Join-Path $env:MONACGRAPH_CACHE "tmp") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $env:MONACGRAPH_CACHE "huggingface") | Out-Null
New-Item -ItemType Directory -Force -Path (Join-Path $env:MONACGRAPH_CACHE "pip") | Out-Null

if (-not $env:HF_HOME) {
    $env:HF_HOME = Join-Path $env:MONACGRAPH_CACHE "huggingface"
}
if (-not $env:HUGGINGFACE_HUB_CACHE) {
    $env:HUGGINGFACE_HUB_CACHE = Join-Path $env:HF_HOME "hub"
}
if (-not $env:TRANSFORMERS_CACHE) {
    $env:TRANSFORMERS_CACHE = $env:HF_HOME
}
if (-not $env:PIP_CACHE_DIR) {
    $env:PIP_CACHE_DIR = Join-Path $env:MONACGRAPH_CACHE "pip"
}

if ($env:MONACGRAPH_KEEP_SYSTEM_TMP -ne "1") {
    $tmp = Join-Path $env:MONACGRAPH_CACHE "tmp"
    $env:TMP = $tmp
    $env:TEMP = $tmp
    $env:TMPDIR = $tmp
    $flag = "-Djava.io.tmpdir=$tmp"
    if (-not $env:JAVA_TOOL_OPTIONS) {
        $env:JAVA_TOOL_OPTIONS = $flag
    } elseif ($env:JAVA_TOOL_OPTIONS -notmatch "java\.io\.tmpdir") {
        $env:JAVA_TOOL_OPTIONS = "$($env:JAVA_TOOL_OPTIONS) $flag"
    }
}
