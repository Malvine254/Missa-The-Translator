const config = {
  MicrosoftAppId: process.env.CLIENT_ID,
  MicrosoftAppType: process.env.BOT_TYPE,
  MicrosoftAppTenantId: process.env.TENANT_ID || process.env.BOT_TENANT_ID || process.env.TEAMS_APP_TENANT_ID,
  MicrosoftAppPassword: process.env.CLIENT_SECRET,
  graphClientId: process.env.GRAPH_CLIENT_ID,
  graphClientSecret: process.env.GRAPH_CLIENT_SECRET,
  azureOpenAIKey: process.env.AZURE_OPENAI_API_KEY,
  azureOpenAIEndpoint: process.env.AZURE_OPENAI_ENDPOINT,
  azureOpenAIDeploymentName: process.env.AZURE_OPENAI_DEPLOYMENT_NAME,
  graphApiEndpoint: process.env.GRAPH_API_ENDPOINT || 'https://graph.microsoft.com/v1.0',
  cognitiveServicesEndpoint: process.env.COGNITIVE_SERVICES_ENDPOINT,
  cognitiveServicesKey: process.env.COGNITIVE_SERVICES_KEY,
  // Bot identity - configurable name for branding
  botDisplayName: process.env.BOT_DISPLAY_NAME || 'Mela AI Meeting Assistant',
  // Optional: a mailbox user ID in your tenant that the bot sends email from.
  // When set, emails are sent from this account (with reply-to set to the requester).
  // This allows sending emails even when the requesting user is in another tenant.
  emailSenderUserId: process.env.EMAIL_SENDER_USER_ID || '',
};

export default config;
