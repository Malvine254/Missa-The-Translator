# Script to simplify bot notifications

$filePath = "src\app\app.ts"
$content = Get-Content $filePath -Raw -Encoding UTF8

# Replace 1: Simplify "I'm now live in the meeting" message  
$old1 = "``🎙️ **I'm now live in the meeting!**\n\nI'm auto-enabling Teams transcription now.\n\nAsk me to:\n• **Transcribe** — see the live transcript so far\n• **Summarize** — recap of the chat\n• **Minutes** — formal meeting minutes``"
$new1 = "``✅ **I've joined the call.**``"
$content = $content.Replace($old1, $new1)
Write-Host "Replace 1 complete"

# Replace 2: Remove "Live transcription is active" notification
$old2 = "``🎧 **Live transcription is active.**``"
$new2 = "``✅ **Transcription started.**``"
if ($content.Contains($old2)) {
    $content = $content.Replace($old2, $new2)
    Write-Host "Replace 2 complete"
} else {
    Write-Host "Replace 2 skipped - pattern not found"
}

# Replace 3: Remove "Live transcript not yet available" notification  
$old3 = "``ℹ️ **Live transcript not yet available from Teams.**\n\nTeams generates transcripts with some delay. I'll keep checking every 10 seconds and save new content as it becomes available.\n\nFor now, you can still ask me questions about the chat conversation.``"
$new3 = "``ℹ️ **Checking for transcript...**``"
if ($content.Contains($old3)) {
    $content = $content.Replace($old3, $new3)
    Write-Host "Replace 3 complete"
} else {
    Write-Host "Replace 3 skipped - pattern not found"
}

# Replace 4: Remove "I've left the meeting" notification
$old4 = "``👋 **I've left the meeting** — all participants have left.\n\nThe transcript will be fetched shortly. You can ask me to show it by saying **transcribe**.``"
if ($content.Contains($old4)) {
    # Remove the entire sendProactiveMessage block
    $beforeMsg = @"
                callEntry.leavingInProgress = true;
                await graphApiHelper.sendProactiveMessage(
                  callEntry.serviceUrl,
                  callEntry.conversationId,
"@
    $afterMsg = @"
                );
              }
              await graphApiHelper.hangUp(callId);
"@
    
    $fullOld = $beforeMsg + "`n                  " + $old4 + "`n" + $afterMsg
    $fullNew = @"
                callEntry.leavingInProgress = true;
                // Silent leave - will notify when transcript is ready
              }
              await graphApiHelper.hangUp(callId);
"@
    if ($content.Contains($fullOld)) {
        $content = $content.Replace($fullOld, $fullNew)
        Write-Host "Replace 4 complete"
    } else {
        Write-Host "Replace 4 skipped - full pattern not found"
    }
}

Set-Content $filePath -Value $content -Encoding UTF8 -NoNewline
Write-Host "All notification updates completed!"
