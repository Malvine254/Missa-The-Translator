# Grant admin consent for Graph API permissions on app 7ed650f2
# Run this as tenant admin

$appId = "7ed650f2-28d9-4c03-b660-2fe0bbb98434"
$tenantId = "588cadf4-9902-4465-86c0-8bcf04f4f102"

Write-Host "Connecting to Microsoft Graph..." -ForegroundColor Cyan
Connect-MgGraph -Scopes "Application.ReadWrite.All", "AppRoleAssignment.ReadWrite.All" -TenantId $tenantId

Write-Host "Finding service principal for app $appId..." -ForegroundColor Cyan
$sp = Get-MgServicePrincipal -Filter "appId eq '$appId'"

if (-not $sp) {
    Write-Host "ERROR: Service principal not found. Creating..." -ForegroundColor Red
    $sp = New-MgServicePrincipal -AppId $appId
}

Write-Host "Service principal found: $($sp.DisplayName) (ID: $($sp.Id))" -ForegroundColor Green

# Get Microsoft Graph service principal
Write-Host "Finding Microsoft Graph service principal..." -ForegroundColor Cyan
$graphSp = Get-MgServicePrincipal -Filter "appId eq '00000003-0000-0000-c000-000000000000'"

Write-Host "Granting application permissions..." -ForegroundColor Cyan

# Define required permissions
$permissions = @(
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
    "Mail.Send",
    "User.Read.All"
)

foreach ($permissionName in $permissions) {
    $appRole = $graphSp.AppRoles | Where-Object { $_.Value -eq $permissionName }
    
    if (-not $appRole) {
        Write-Host "  ⚠️  Permission '$permissionName' not found in Graph API" -ForegroundColor Yellow
        continue
    }
    
    # Check if already granted
    $existing = Get-MgServicePrincipalAppRoleAssignment -ServicePrincipalId $sp.Id | 
        Where-Object { $_.AppRoleId -eq $appRole.Id -and $_.ResourceId -eq $graphSp.Id }
    
    if ($existing) {
        Write-Host "  ✅ $permissionName - already granted" -ForegroundColor Gray
    } else {
        try {
            New-MgServicePrincipalAppRoleAssignment `
                -ServicePrincipalId $sp.Id `
                -PrincipalId $sp.Id `
                -ResourceId $graphSp.Id `
                -AppRoleId $appRole.Id | Out-Null
            Write-Host "  ✅ $permissionName - GRANTED" -ForegroundColor Green
        } catch {
            Write-Host "  ❌ $permissionName - FAILED: $_" -ForegroundColor Red
        }
    }
}

Write-Host "`nAdmin consent granted successfully!" -ForegroundColor Green
Write-Host "Redeploy your bot for changes to take effect." -ForegroundColor Cyan
