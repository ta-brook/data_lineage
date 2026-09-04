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

.PARAMETER Mode
  sync (default) | list | close | reopen
.PARAMETER Id
  Ticket id for close/reopen, e.g. EXE-01
.EXAMPLE
  powershell -File scripts/sync-tickets.ps1 --sync
  powershell -File scripts/sync-tickets.ps1 --list
  powershell -File scripts/sync-tickets.ps1 --close EXE-01
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

function Save-Manifest {
  $json = $manifest | ConvertTo-Json -Depth 6
  # UTF-8 without BOM (PS 5.1 Set-Content -Encoding utf8 writes a BOM and can mangle non-ASCII)
  [System.IO.File]::WriteAllText($manifestPath, $json, (New-Object System.Text.UTF8Encoding($false)))
}

function Find-IssueNumber([string]$ticketId) {
  $found = & $gh issue list --repo $repo --search "in:title $ticketId" --state all --json number,title 2>$null | ConvertFrom-Json
  if ($found) {
    $hit = $found | Where-Object { $_.title.Contains("[$ticketId]") } | Select-Object -First 1
    if ($hit) { return [int]$hit.number }
  }
  return $null
}

function New-Issue($t) {
  $title = "[$($t.id)] $($t.title)"
  $labels = ($t.labels -join ",")
  $out = & $gh issue create --repo $repo --title $title --body $t.body --label $labels --assignee $assignee 2>&1
  if ($out -match "issues/(\d+)") {
    $t.github_number = [int]$Matches[1]
    Write-Output "created $($t.id) -> #$($t.github_number)"
  } else {
    Write-Warning "create failed for $($t.id): $out"
  }
}

function Update-Issue($t) {
  $title = "[$($t.id)] $($t.title)"
  $labels = ($t.labels -join ",")
  & $gh issue edit $t.github_number --repo $repo --title $title --body $t.body --add-label $labels --add-assignee $assignee 2>$null | Out-Null
  $state = (& $gh issue view $t.github_number --repo $repo --json state --jq '.state' 2>$null).Trim()
  if ($t.status -eq "closed" -and $state -ne "CLOSED") {
    & $gh issue close $t.github_number --repo $repo 2>$null | Out-Null
  } elseif ($t.status -ne "closed" -and $state -eq "CLOSED") {
    & $gh issue reopen $t.github_number --repo $repo 2>$null | Out-Null
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
        if ($t.github_number) { & $gh issue close $t.github_number --repo $repo 2>&1 | Out-Null }
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
        if ($t.github_number) { & $gh issue reopen $t.github_number --repo $repo 2>&1 | Out-Null }
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
    $issues = & $gh issue list --repo $repo --state all --limit 20 --json number,title,labels 2>$null | ConvertFrom-Json
    foreach ($i in $issues) {
      $lbls = ($i.labels | ForEach-Object { $_.name }) -join ","
      Write-Output ("#{0} {1} [{2}]" -f $i.number, $i.title, $lbls)
    }
  }
}