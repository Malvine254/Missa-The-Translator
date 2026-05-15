import "./operatorConsole";
import app from "./app/app";

// Start the application
(async () => {
  await app.start(process.env.PORT || process.env.port || 3978);
  console.log(`[STARTUP] Agent listening on port ${process.env.PORT || process.env.port || 3978}`);

  const botEndpoint = process.env.BOT_ENDPOINT;
  if (botEndpoint) {
    const callbackUri = `${botEndpoint}/api/calls`;
    console.log(`[STARTUP] Calls callback configured: ${callbackUri}`);
  }
})();
