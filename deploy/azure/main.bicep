// FluxDB on Azure Container Apps.
//
// Deployed by .github/workflows/deploy-azure.yml on every push to master. The
// shared environment, the Azure Files share and the deployment identity are
// created once by deploy/azure/setup.sh; this template only owns the app.
//
// Cost model: the app scales to zero 30 minutes after the last request and is
// billed nothing while it is at zero; the next visit starts it again in a few
// seconds. During the optional warm-up window after the first deploy, a cron
// rule holds one replica running whether or not anyone is visiting.

@description('Container image to run, for example ghcr.io/owner/fluxdb:<commit sha>.')
param image string

@description('Name of the existing Container Apps environment.')
param environmentName string

@description('Region of that environment; the app must be in the same one.')
param location string = resourceGroup().location

@description('Name of the container app; also the first label of its public hostname.')
param appName string = 'fluxdb'

@description('Environment storage definition of the Azure Files share that holds data archives.')
param storageName string = 'fluxdb-data'

@description('Seconds without a request before the app scales to zero.')
@minValue(300)
param idleShutdownSeconds int = 1800

@description('Warm-up window, as UTC cron expressions "minute hour day month *". Empty once it has passed.')
param warmupCronStart string = ''
param warmupCronEnd string = ''

@description('Warm-up window as ISO timestamps, recorded as tags so later deploys keep the same window.')
param warmupFrom string = ''
param warmupUntil string = ''

@secure()
@description('Administration token for /api/v1. At least 32 characters.')
param fluxdbToken string

@secure()
@description('Signs OAuth state and cookies.')
param sessionSecret string

@secure()
@description('Optional Postgres URL for accounts and projects. Empty keeps them in SQLite inside the archived data directory.')
param databaseUrl string = ''

@description('Optional GitHub OAuth app. Callback: <app URL>/api/cloud/auth/github/callback')
param githubClientId string = ''

@secure()
param githubClientSecret string = ''

@secure()
param geminiApiKey string = ''

param geminiModel string = 'gemini-2.5-flash'

resource environment 'Microsoft.App/managedEnvironments@2025-01-01' existing = {
  name: environmentName
}

// Known before the app exists, so PUBLIC_BASE_URL is right on the first deploy.
var origin = 'https://${appName}.${environment.properties.defaultDomain}'
var githubOAuth = !empty(githubClientId) && !empty(githubClientSecret)

var secrets = concat(
  [
    { name: 'fluxdb-token', value: fluxdbToken }
    { name: 'session-secret', value: sessionSecret }
  ],
  empty(databaseUrl) ? [] : [{ name: 'database-url', value: databaseUrl }],
  githubOAuth ? [{ name: 'github-client-secret', value: githubClientSecret }] : [],
  empty(geminiApiKey) ? [] : [{ name: 'gemini-api-key', value: geminiApiKey }]
)

var settings = concat(
  [
    { name: 'FLUXDB_ADDR', value: '0.0.0.0:8086' }
    // Local disk, archived to PERSIST_DIR by fluxdb-persist (persist-datadir.sh).
    { name: 'FLUXDB_DATA_DIR', value: '/app/data' }
    { name: 'FLUXDB_STATIC_DIR', value: '/app/web' }
    { name: 'PERSIST_DIR', value: '/persist' }
    { name: 'PUBLIC_BASE_URL', value: origin }
    { name: 'FLUXDB_TOKEN', secretRef: 'fluxdb-token' }
    { name: 'FLUXDB_SESSION_SECRET', secretRef: 'session-secret' }
    { name: 'GEMINI_MODEL', value: geminiModel }
  ],
  empty(databaseUrl) ? [] : [{ name: 'DATABASE_URL', secretRef: 'database-url' }],
  githubOAuth
    ? [
        { name: 'GITHUB_CLIENT_ID', value: githubClientId }
        { name: 'GITHUB_CLIENT_SECRET', secretRef: 'github-client-secret' }
      ]
    : [],
  empty(geminiApiKey) ? [] : [{ name: 'GEMINI_API_KEY', secretRef: 'gemini-api-key' }]
)

var warmupRules = empty(warmupCronStart)
  ? []
  : [
      {
        name: 'warm-up'
        custom: {
          type: 'cron'
          metadata: {
            timezone: 'Etc/UTC'
            start: warmupCronStart
            end: warmupCronEnd
            desiredReplicas: '1'
          }
        }
      }
    ]

resource app 'Microsoft.App/containerApps@2025-01-01' = {
  name: appName
  location: location
  tags: union(
    { project: 'fluxdb' },
    empty(warmupUntil) ? {} : { 'warmup-from': warmupFrom, 'warmup-until': warmupUntil }
  )
  properties: {
    environmentId: environment.id
    configuration: {
      activeRevisionsMode: 'Single'
      maxInactiveRevisions: 3
      ingress: {
        external: true
        targetPort: 8086
        transport: 'auto'
        allowInsecure: false
      }
      secrets: secrets
    }
    template: {
      // Room for the server to flush its memtables and the last archive to upload.
      terminationGracePeriodSeconds: 120
      containers: [
        {
          name: 'fluxdb'
          image: image
          resources: {
            cpu: json('0.25')
            memory: '0.5Gi'
          }
          env: settings
          volumeMounts: [
            {
              volumeName: 'persist'
              mountPath: '/persist'
            }
          ]
          probes: [
            {
              type: 'Startup'
              httpGet: {
                path: '/health'
                port: 8086
              }
              initialDelaySeconds: 2
              periodSeconds: 5
              timeoutSeconds: 3
              failureThreshold: 10
            }
            {
              type: 'Readiness'
              httpGet: {
                path: '/health'
                port: 8086
              }
              periodSeconds: 10
              timeoutSeconds: 3
              failureThreshold: 3
            }
            {
              type: 'Liveness'
              httpGet: {
                path: '/health'
                port: 8086
              }
              periodSeconds: 30
              timeoutSeconds: 5
              failureThreshold: 3
            }
          ]
        }
      ]
      scale: {
        // One replica at most: a data directory has a single owner.
        minReplicas: 0
        maxReplicas: 1
        cooldownPeriod: idleShutdownSeconds
        pollingInterval: 30
        rules: concat(
          [
            {
              name: 'http'
              http: {
                metadata: {
                  concurrentRequests: '50'
                }
              }
            }
          ],
          warmupRules
        )
      }
      volumes: [
        {
          name: 'persist'
          storageType: 'AzureFile'
          storageName: storageName
          // The container runs as the image's `fluxdb` user (uid 10001).
          mountOptions: 'dir_mode=0777,file_mode=0777,uid=10001,gid=10001'
        }
      ]
    }
  }
}

output url string = origin
output latestRevision string = app.properties.latestRevisionName
