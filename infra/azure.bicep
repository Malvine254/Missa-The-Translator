@maxLength(20)
@minLength(4)
@description('Used to generate names for all resources in this file')
param resourceBaseName string

@secure()
param azureOpenAIKey string

@secure()
param azureOpenAIEndpoint string

@secure()
param azureOpenAIDeploymentName string

@description('Client ID for the Graph API app registration (separate from bot identity)')
param graphClientId string

@description('Client secret for the Graph API app registration')
@secure()
param graphClientSecret string

@description('Azure Cognitive Services endpoint (optional, for speech-to-text)')
param cognitiveServicesEndpoint string = ''

@description('Azure Cognitive Services key (optional, for speech-to-text)')
@secure()
param cognitiveServicesKey string = ''

@description('Mailbox user ID to send AI-generated emails from (optional). If empty, sender falls back to requesting user.')
param emailSenderUserId string = ''

param webAppSKU string

@maxLength(42)
param botDisplayName string

param serverfarmsName string = resourceBaseName
param webAppName string = resourceBaseName
param identityName string = resourceBaseName
param location string = resourceGroup().location

resource identity 'Microsoft.ManagedIdentity/userAssignedIdentities@2023-01-31' = {
  location: location
  name: identityName
}

// Compute resources for your Web App
resource serverfarm 'Microsoft.Web/serverfarms@2021-02-01' = {
  kind: 'app'
  location: location
  name: serverfarmsName
  sku: {
    name: webAppSKU
  }
}

// Web App that hosts your agent
resource webApp 'Microsoft.Web/sites@2021-02-01' = {
  kind: 'app'
  location: location
  name: webAppName
  properties: {
    serverFarmId: serverfarm.id
    httpsOnly: true
    siteConfig: {
      alwaysOn: true
      appSettings: [
        {
          name: 'WEBSITE_RUN_FROM_PACKAGE'
          value: '1' // Run Azure App Service from a package file
        }
        {
          name: 'WEBSITE_NODE_DEFAULT_VERSION'
          value: '~20' // Set NodeJS version to 20.x for your site
        }
        {
          name: 'RUNNING_ON_AZURE'
          value: '1'
        }
        {
          name: 'CLIENT_ID'
          value: graphClientId
        }
        {
          name: 'CLIENT_SECRET'
          value: graphClientSecret
        }
        {
          name: 'TENANT_ID'
          value: tenant().tenantId
        }
        { 
          name: 'BOT_TYPE' 
          value: 'MultiTenant'
        }
        {
          name: 'GRAPH_CLIENT_ID'
          value: graphClientId
        }
        {
          name: 'GRAPH_CLIENT_SECRET'
          value: graphClientSecret
        }
        {
          name: 'AZURE_OPENAI_API_KEY'
          value: azureOpenAIKey
        }
        {
          name: 'AZURE_OPENAI_ENDPOINT'
          value: azureOpenAIEndpoint
        }
        {
          name: 'AZURE_OPENAI_DEPLOYMENT_NAME'
          value: azureOpenAIDeploymentName
        }
        {
          name: 'BOT_ENDPOINT'
          value: 'https://${webAppName}.azurewebsites.net'
        }
        {
          name: 'COGNITIVE_SERVICES_ENDPOINT'
          value: cognitiveServicesEndpoint
        }
        {
          name: 'COGNITIVE_SERVICES_KEY'
          value: cognitiveServicesKey
        }
        {
          name: 'EMAIL_SENDER_USER_ID'
          value: emailSenderUserId
        }
      ]
      ftpsState: 'FtpsOnly'
    }
  }
  identity: {
    type: 'UserAssigned'
    userAssignedIdentities: {
      '${identity.id}': {}
    }
  }
}

// Register your web service as a bot with the Bot Framework
module azureBotRegistration './botRegistration/azurebot.bicep' = {
  name: 'Azure-Bot-registration'
  params: {
    resourceBaseName: resourceBaseName
    botAppDomain: webApp.properties.defaultHostName
    botDisplayName: botDisplayName
    botId: graphClientId
  }
}

// The output will be persisted in .env.{envName}. Visit https://aka.ms/teamsfx-actions/arm-deploy for more details.
output BOT_AZURE_APP_SERVICE_RESOURCE_ID string = webApp.id
output BOT_DOMAIN string = webApp.properties.defaultHostName
output BOT_ENDPOINT string = 'https://${webApp.properties.defaultHostName}'
output BOT_ID string = graphClientId
output BOT_TENANT_ID string = tenant().tenantId
