$ErrorActionPreference = "Stop"

$repo = "ThinkHao/nfatool"
$branch = "main"
$message = "feat: improve task group management UX"
$cwd = "C:/Users/haoji/Desktop/Code/nfatool"
$files = @(
  "server/models.py",
  "server/schemas.py",
  "server/static/app.js",
  "server/static/index.html",
  "server/tests/test_task_group_management.py"
)

foreach ($rel in $files) {
  $apiPath = "repos/$repo/contents/$rel"
  $sha = ""
  if ($rel -ne "server/tests/test_task_group_management.py") {
    $shaOut = gh api "${apiPath}?ref=$branch" --jq .sha
    if ($LASTEXITCODE -eq 0) { $sha = "$shaOut".Trim() }
  }

  $abs = Join-Path $cwd $rel
  $b64 = [System.Convert]::ToBase64String([System.IO.File]::ReadAllBytes($abs))
  $payload = @{ message = $message; content = $b64; branch = $branch }
  if ($sha) { $payload.sha = $sha }

  $tmp = Join-Path $cwd "tmp_put_payload.json"
  $payload | ConvertTo-Json -Depth 4 | Set-Content -Encoding Ascii $tmp
  $ok = $false
  for ($i = 0; $i -lt 3; $i++) {
    $null = gh api $apiPath -X PUT --input $tmp
    if ($LASTEXITCODE -eq 0) { $ok = $true; break }
    Start-Sleep -Seconds 2
  }
  if (-not $ok) { throw "update failed: $rel" }
  Remove-Item $tmp -Force
  Write-Output ("updated=" + $rel)
}

$head = gh api "repos/$repo/git/ref/heads/$branch" --jq .object.sha
Write-Output ("head_sha=" + $head)
