// ============================================================
// The shared Container Apps environment
// ============================================================
// Created once by setup.sh and shared with CodeLens. It is asked for
// as a standard (workload profiles) environment by name: the CLI's
// default is now "express", which cannot mount the Azure Files
// share the apps keep their data on, or run the cron scale rule
// FluxDB's warm-up uses.
//
// No log destination is configured, so nothing is stored in, or
// billed by, Log Analytics; logs are streamed on demand instead.
// ============================================================

param name string
param location string
param tags object = {}

resource environment 'Microsoft.App/managedEnvironments@2026-07-01' = {
  name: name
  location: location
  tags: tags
  properties: {
    environmentMode: 'WorkloadProfiles'
    workloadProfiles: [
      {
        name: 'Consumption'
        workloadProfileType: 'Consumption'
      }
    ]
  }
}

output environmentMode string = environment.properties.environmentMode
