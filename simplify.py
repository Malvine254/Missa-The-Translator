with open('src/app/app.ts', 'r', encoding='utf-8') as f:
    content = f.read()

# Exact match from the file
content = content.replace(
    "`🎙️ **I'm now live in the meeting!**\\n\\nI'm auto-enabling Teams transcription now.\\n\\nAsk me to:\\n• **Transcribe** — see the live transcript so far\\n• **Summarize** — recap of the chat\\n• **Minutes** — formal meeting minutes`",
    "`✅ **I've joined the call.**`"
)

content = content.replace(
    "`🎧 **Live transcription is active.**`",
    "// Transcription started silently"
)

content = content.replace(
    "`ℹ️ **Live transcript not yet available from Teams.**\\n\\nTeams generates transcripts with some delay. I'll keep checking every 10 seconds and save new content as it becomes available.\\n\\nFor now, you can still ask me questions about the chat conversation.`",
    "// Polling for transcript silently"
)

content = content.replace(
    "`👋 **I've left the meeting** — all participants have left.\\n\\nThe transcript will be fetched shortly. You can ask me to show it by saying **transcribe**.`"
,
    "// Left meeting silently"
)

with open('src/app/app.ts', 'w', encoding='utf-8', newline='') as f:
    f.write(content)

print("✅ All notifications simplified!")
