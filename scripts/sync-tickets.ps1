<#
.SYNOPSIS
  Syncs scripts/tickets.json to GitHub issues (create/update/close) using gh.

.DESCRIPTION
  The manifest (scripts/tickets.json) is the source of truth for POC tickets.
  This script upserts one GitHub issue per ticket:
    - creates missing issues (title "[ID] <title>", labels, assignee)
    - updates labels/assignee/title/body of existing issues
    - closes/reopens issues per the manifest status
  It writes the GitHub issue number back into tickets.json.

  PS 5.1 note: use -Mode <mode> (not --mode). With `powershell -File`, the
  --sync/--list/--close forms arrive as positional strings and are normalized
  here, but -Mode is the documented, reliable form.

.PARAMETER Mode
  sync (default) | list | close | reopen
.PARAMETER Id
  Ticket id for close/reopen, e.g. EXE-01
.EXAMPLE
  powershell -File scripts/sync-tickets.ps1 -Mode sync
  powershell -File scripts/sync-tickets.ps1 -Mode list
  powershell -File scripts/sync-tickets.ps1 -Mode close -Id EXE-01
#>
param(
  [string]$Mode = "sync",
  [string]$Id = ""
)

$ErrorActionPreference = "Stop"

# Locate gh (PATH first, fall back to the local install used by the POC)
$gh = (Get-Command gh -ErrorAction SilentlyContinue).Source
if (-not $gh) { $gh = "C:\Users\user\AppData\Local\Programs\gh\bin\gh.exe" }
if (-not (Test-Path $gh)) { throw "gh CLI not found. Install it or set GH_TOKEN." }

$manifestPath = Join-Path $PSScriptRoot "tickets.json"
$manifest = Get-Content $manifestPath -Raw -Encoding UTF8 | ConvertFrom-Json
$repo = $manifest.repo
$assignee = $manifest.assignee

# Normalize -Mode: accept -Mode sync, --sync, -sync. PS 5.1 -File invocation
# passes --sync/--list/--close as positional strings (they do not bind to the
# named -Mode parameter), so strip leading dashes before the switch.
$Mode = $Mode.TrimStart('-')

function Save-Manifest {
  $json = $manifest | ConvertTo-Json -Depth 6
  # UTF-8 without BOM (PS 5.1 Set-Content -Encoding utf8 writes a BOM and can mangle non-ASCII)
  [System.IO.File]::WriteAllText($manifestPath, $json, (New-Object System.Text.UTF8Encoding($false)))
}

function Invoke-Gh {
  # Runs gh with stderr suppressed. Under $ErrorActionPreference="Stop", PS 5.1
  # turns native stderr into a terminating NativeCommandError (gh writes status
  # lines like "Closed issue #N" to stderr), which aborts the whole sync. The
  # EAP=Continue scope + 2>$null keeps the run alive; the process exit code is
  # captured for callers.
  param([Parameter(ValueFromRemainingArguments = $true)][string[]]$GhArgs)
  $ErrorActionPreference = "Continue"
  $out = & $gh @GhArgs 2>$null
  $script:ghExitCode = $LASTEXITCODE
  $ErrorActionPreference = "Stop"
  return $out
}

function Find-IssueNumber([string]$ticketId) {
  $found = Invoke-Gh issue list --repo $repo --search "in:title $ticketId" --state all --json number,title | ConvertFrom-Json
  if ($found) {
    $hit = $found | Where-Object { $_.title.Contains("[$ticketId]") } | Select-Object -First 1
    if ($hit) { return [int]$hit.number }
  }
  return $null
}

function New-Issue($t) {
  $title = "[$($t.id)] $($t.title)"
  $labels = ($t.labels -join ",")
  # Body via --body-file: PS 5.1 does not escape embedded double quotes when
  # passing a string to a native command, which corrupts --body (e.g. a body
  # containing `$ErrorActionPreference="Stop"`). A temp file avoids the issue.
  $bodyFile = Join-Path $env:TEMP ("cln-body-{0}.md" -f $t.id)
  [System.IO.File]::WriteAllText($bodyFile, $t.body, (New-Object System.Text.UTF8Encoding($false)))
  $out = Invoke-Gh issue create --repo $repo --title $title --body-file $bodyFile --label $labels --assignee $assignee
  Remove-Item $bodyFile -ErrorAction SilentlyContinue
  if ($script:ghExitCode -ne 0) {
    Write-Warning "create failed for $($t.id) (exit $($script:ghExitCode)): $out"
    return
  }
  if ($out -match "issues/(\d+)") {
    $t.github_number = [int]$Matches[1]
    Write-Output "created $($t.id) -> #$($t.github_number)"
    # Manifest status is authoritative: a ticket added as already-closed must
    # not be left open on GitHub.
    if ($t.status -eq "closed") {
      Invoke-Gh issue close $t.github_number --repo $repo | Out-Null
      Write-Output "closed $($t.id) (#$($t.github_number)) per manifest status"
    }
  } else {
    Write-Warning "create failed for $($t.id): $out"
  }
}

function Update-Issue($t) {
  $title = "[$($t.id)] $($t.title)"
  $labels = ($t.labels -join ",")
  # Body via --body-file (see New-Issue for the PS 5.1 quoting rationale).
  $bodyFile = Join-Path $env:TEMP ("cln-body-{0}.md" -f $t.id)
  [System.IO.File]::WriteAllText($bodyFile, $t.body, (New-Object System.Text.UTF8Encoding($false)))
  Invoke-Gh issue edit $t.github_number --repo $repo --title $title --body-file $bodyFile --add-label $labels --add-assignee $assignee | Out-Null
  Remove-Item $bodyFile -ErrorAction SilentlyContinue
  $state = (Invoke-Gh issue view $t.github_number --repo $repo --json state --jq '.state').Trim()
  if ($t.status -eq "closed" -and $state -ne "CLOSED") {
    Invoke-Gh issue close $t.github_number --repo $repo | Out-Null
  } elseif ($t.status -ne "closed" -and $state -eq "CLOSED") {
    Invoke-Gh issue reopen $t.github_number --repo $repo | Out-Null
  }
  Write-Output "synced $($t.id) (#$($t.github_number))"
}

switch ($Mode.ToLower()) {
  "list" {
    foreach ($t in $manifest.tickets) {
      $num = if ($t.github_number) { "#$($t.github_number)" } else { "-" }
      Write-Output ("{0,-8} {1,-5} {2,-8} {3}" -f $t.id, $num, $t.status, $t.title)
    }
  }
  "close" {
    foreach ($t in $manifest.tickets) {
      if ($t.id -eq $Id) {
        if (-not $t.github_number) { $t.github_number = Find-IssueNumber $t.id }
        if ($t.github_number) { Invoke-Gh issue close $t.github_number --repo $repo | Out-Null }
        $t.status = "closed"
        Write-Output "closed $($t.id)"
      }
    }
    Save-Manifest
  }
  "reopen" {
    foreach ($t in $manifest.tickets) {
      if ($t.id -eq $Id) {
        if (-not $t.github_number) { $t.github_number = Find-IssueNumber $t.id }
        if ($t.github_number) { Invoke-Gh issue reopen $t.github_number --repo $repo | Out-Null }
        $t.status = "open"
        Write-Output "reopened $($t.id)"
      }
    }
    Save-Manifest
  }
  default {
    foreach ($t in $manifest.tickets) {
      if (-not $t.github_number) { $t.github_number = Find-IssueNumber $t.id }
      if ($t.github_number) { Update-Issue $t } else { New-Issue $t }
    }
    Save-Manifest
    Write-Output "---"
    $issues = Invoke-Gh issue list --repo $repo --state all --limit 20 --json number,title,labels | ConvertFrom-Json
    foreach ($i in $issues) {
      $lbls = ($i.labels | ForEach-Object { $_.name }) -join ","
      Write-Output ("#{0} {1} [{2}]" -f $i.number, $i.title, $lbls)
    }
  }
}