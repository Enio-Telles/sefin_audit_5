# ==========================================
# Setup Environment - sefin_audit_5
# ==========================================
# Script para configurar Git e GitHub CLI portáteis no PATH
# Rode este script antes de usar git, ou use o setup.cmd

$projectRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$gitPortable = Join-Path $projectRoot "Git-Portable\cmd"
$ghPortable = Join-Path $projectRoot "gh-portable\bin"

# Adicionar ao PATH da sessão
$env:PATH = "$gitPortable;$ghPortable;" + $env:PATH

Write-Host "✅ Ambiente configurado!" -ForegroundColor Green
Write-Host "   Git: $gitPortable"
Write-Host "   GitHub CLI: $ghPortable"

# Verificar disponibilidade
git --version | Write-Host
gh --version | Write-Host
