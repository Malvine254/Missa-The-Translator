# Python script to simplify bot notifications

filepath = r"src\app\app.ts"

# Read file with UTF-8 encoding
with open(filepath, 'r', encoding='utf-8') as f:
    content = f.read()

# Replace 1: Simplify "I'm now live in the meeting" message
old1 = "`🎙️ **I'm now live in the meeting!**\\n\\nI'm auto-enabling Teams transcription now.\\n\\nAsk me to:\\n• **Transcribe** — see the live transcript so far\\n• **Summarize** — recap of the chat\\n• **Minutes** — formal meeting minutes`"
new1 = "`✅ **I've joined the call.**`"
if old1 in content:
    content = content.replace(old1, new1)
    print("Replace 1: Meeting join notification - DONE")
else:
    print("Replace 1: Pattern not found in file")

# Replace 2: Remove "Live transcription is active" notification  
old2 = "`🎧 **Live transcription is active.**`"
if old2 in content:
    content = content.replace(old2, "// Silently started")
    print("Replace 2: Transcription active notification - DONE")
else:
    print("Replace 2: Pattern not found")

# Replace 3: Remove "Live transcript not yet available" notification
old3 = "`ℹ️ **Live transcript not yet available from Teams.**\\n\\nTeams generates transcripts with some delay. I'll keep checking every 10 seconds and save new content as it becomes available.\\n\\nFor now, you can still ask me questions about the chat conversation.`"
if old3 in content:
    content = content.replace(old3, "// Checking silently")
    print("Replace 3: Transcript delay notification - DONE")
else:
    print("Replace 3: Pattern not found")

# Replace 4: Remove "I've left the meeting" notification
old4 = "`👋 **I've left the meeting** — all participants have left.\\n\\nThe transcript will be fetched shortly. You can ask me to show it by saying **transcribe**.`"
if old4 in content:
    content = content.replace(old4, "// Silent leave")
    print("Replace 4: Meeting leave notification - DONE")
else:
    print("Replace 4: Pattern not found")

# Write back with UTF-8 encoding
with open(filepath, 'w', encoding='utf-8', newline='') as f:
    f.write(content)

print("All updates completed!")
