Write-Host "=== Khoi dong TimescaleDB container ==="
docker compose up -d postgres
if ($LASTEXITCODE -ne 0) {
    throw "docker compose up failed"
}

Write-Host "=== Cho PostgreSQL san sang ==="
$ready = $false
while (-not $ready) {
    $result = docker exec stock-pipeline-db pg_isready -U stock -d stock_pipeline 2>&1
    if ($LASTEXITCODE -eq 0) {
        $ready = $true
    } else {
        Write-Host "  Waiting..."
        Start-Sleep -Seconds 2
    }
}
Start-Sleep -Seconds 2

Write-Host "=== Apply DDL ==="
Get-Content warehouse\ddl\schema.sql -Raw -Encoding UTF8 | docker exec -i stock-pipeline-db psql -U stock -d stock_pipeline
if ($LASTEXITCODE -ne 0) {
    throw "DDL apply failed"
}

Write-Host "=== Done. DATABASE_URL ==="
Write-Host "postgresql://stock:stock@localhost:55432/stock_pipeline"
