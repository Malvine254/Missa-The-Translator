# Remove admin consent for Graph API permissions from app 7ed650f2
# Run this as tenant admin

$appId = "7ed650f2-28d9-4c03-b660-2fe0bbb98434"
$tenantId = "588cadf4-9902-4465-86c0-8bcf04f4f102"

Write-Host "Connecting to Microsoft Graph..." -ForegroundColor Cyan
Connect-MgGraph -Scopes "Application.ReadWrite.All", "AppRoleAssignment.ReadWrite.All" -TenantId $tenantId

Write-Host "Finding service principal for app $appId..." -ForegroundColor Cyan
$sp = Get-MgServicePrincipal -Filter "appId eq '$appId'"

if (-not $sp) {
    Write-Host "ERROR: Service principal not found." -ForegroundColor Red
    exit 1
}

Write-Host "Service principal found: $($sp.DisplayName) (ID: $($sp.Id))" -ForegroundColor Green

# Get Microsoft Graph service principal
Write-Host "Finding Microsoft Graph service principal..." -ForegroundColor Cyan
$graphSp = Get-MgServicePrincipal -Filter "appId eq '00000003-0000-0000-c000-000000000000'"

Write-Host "`nRemoving application permissions..." -ForegroundColor Yellow

# Define permissions to remove (same list that was added)
$permissionsToRemove = @(
    "OnlineMeetings.Read.All",
    "OnlineMeetingTranscript.Read.All", 
    "Chat.Read.All",
    "ChatSettings.Read.Chat",
    "ChatMessage.Read.Chat",
    "ChatMember.Read.Chat",
    "Calls.JoinGroupCall.All",
    "Calls.AccessMedia.All",
    "Calls.Initiate.All",
    "CallRecords.Read.All",
    "Calendars.Read",
    "Mail.Read",
    "Mail.Send",
    "MailboxSettings.Read",
    "User.Read.All"
)

# Get all current app role assignments
$currentAssignments = Get-MgServicePrincipalAppRoleAssignment -ServicePrincipalId $sp.Id

Write-Host "`nCurrent assignments found: $($currentAssignments.Count)" -ForegroundColor Cyan

foreach ($permissionName in $permissionsToRemove) {
    $appRole = $graphSp.AppRoles | Where-Object { $_.Value -eq $permissionName }
    
    if (-not $appRole) {
        Write-Host "  ⚠️  Permission '$permissionName' not found in Graph API" -ForegroundColor Yellow
        continue
    }
    
    # Find existing assignment
    $existing = $currentAssignments | 
        Where-Object { $_.AppRoleId -eq $appRole.Id -and $_.ResourceId -eq $graphSp.Id }
    
    if ($existing) {
        try {
            Remove-MgServicePrincipalAppRoleAssignment `
                -ServicePrincipalId $sp.Id `
                -AppRoleAssignmentId $existing.Id
            Write-Host "  🗑️  $permissionName - REMOVED" -ForegroundColor Green
        } catch {
            Write-Host "  ❌ $permissionName - FAILED TO REMOVE: $_" -ForegroundColor Red
        }
    } else {
        Write-Host "  ⏭️  $permissionName - not assigned, skipping" -ForegroundColor Gray
    }
}

Write-Host "`n✅ Permission removal complete!" -ForegroundColor Green
Write-Host "The app now has reduced permissions." -ForegroundColor Cyan

# Show remaining permissions
Write-Host "`nRemaining permissions:" -ForegroundColor Cyan
$remaining = Get-MgServicePrincipalAppRoleAssignment -ServicePrincipalId $sp.Id
if ($remaining.Count -eq 0) {
    Write-Host "  (none)" -ForegroundColor Gray
} else {
    foreach ($r in $remaining) {
        $roleName = ($graphSp.AppRoles | Where-Object { $_.Id -eq $r.AppRoleId }).Value
        Write-Host "  - $roleName" -ForegroundColor White
    }
}
