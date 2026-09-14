# Ingests the official HotpotQA Scott Derrickson / Ed Wood example and runs HippoRAG 1.
$ErrorActionPreference = "Stop"
. "$PSScriptRoot\env.ps1"
Set-Location $Root

$EmbeddingUri = if ($env:MONACGRAPH_EMBEDDING_URI) { $env:MONACGRAPH_EMBEDDING_URI } else { "http://127.0.0.1:8099/" }
$ExtractorUrl = if ($env:MONACGRAPH_EXTRACTOR_URL) { $env:MONACGRAPH_EXTRACTOR_URL } else { "http://127.0.0.1:11434/v1/chat/completions" }
$Model = if ($env:MONACGRAPH_LLM_MODEL) { $env:MONACGRAPH_LLM_MODEL } else { "qwen3:0.6b" }
$DbName = if ($env:MONACGRAPH_DB) { $env:MONACGRAPH_DB } else { "hotpotqa-demo-" + (Get-Date -Format "yyyyMMddHHmmss") }
$Input = Join-Path $Root "data\hotpotqa-smoke\scott-derrickson-ed-wood.txt"
$Question = "Were Scott Derrickson and Ed Wood of the same nationality?"
$Jar = Join-Path $Root "target\Gremmunity-1.0-SNAPSHOT.jar"

function Test-Http($url) {
    try {
        Invoke-WebRequest -UseBasicParsing -TimeoutSec 3 -Uri $url | Out-Null
        return $true
    } catch {
        return $false
    }
}

$embeddingHealth = $EmbeddingUri.TrimEnd("/") + "/health"
if (-not (Test-Http $embeddingHealth)) {
    throw @"
Embedding service is not reachable at $embeddingHealth
Start it first:

  powershell -ExecutionPolicy Bypass -File .\scripts\start-embedding.ps1
"@
}

if (-not (Test-Http "http://127.0.0.1:11434/api/tags")) {
    throw @"
Ollama is not reachable at http://127.0.0.1:11434
Install Ollama, then:

  ollama pull $Model
"@
}

if (-not (Test-Path $Jar)) {
    Write-Host "Packaging Java application (mvn package)"
    & mvn -q -DskipTests package
    if ($LASTEXITCODE -ne 0) {
        throw "mvn package failed"
    }
}

Write-Host "Using new graph --db $DbName (do not reopen an empty-start JNI graph)"
& java -cp $Jar db.monacgraph.ingestion.IngestionCli `
    --db $DbName `
    --input $Input `
    --extractor-url $ExtractorUrl `
    --extractor-model $Model `
    --embedding-uri $EmbeddingUri `
    --query $Question
if ($LASTEXITCODE -ne 0) {
    throw "IngestionCli failed"
}
