import app from "./app/app";

// Start the application
(async () => {
  await app.start(process.env.PORT || process.env.port || 3978);
  console.log(`\nAgent started, app listening to`, process.env.PORT || process.env.port || 3978);

  // Remind about calling webhook if BOT_ENDPOINT is set
  const botEndpoint = process.env.BOT_ENDPOINT;
  if (botEndpoint) {
    const callbackUri = `${botEndpoint}/api/calls`;
    console.log(`\n[CALLS_CONFIG] ⚠️  Calling webhook URL for this session:`);
    console.log(`[CALLS_CONFIG]    ${callbackUri}`);
    console.log(`[CALLS_CONFIG]    Update this in Azure Portal → Bot Services → Channels → Microsoft Teams → Calling tab`);
    console.log(`[CALLS_CONFIG]    if it differs from the last registered value.\n`);
  }
})();
