import { contextBridge, ipcRenderer } from 'electron'

// Expose protected methods to the renderer process
contextBridge.exposeInMainWorld('electronAPI', {
  getAppVersion: () => ipcRenderer.invoke('get-app-version'),
  getPlatform: () => ipcRenderer.invoke('get-platform'),
  
  // Window controls
  minimize: () => ipcRenderer.send('minimize'),
  maximize: () => ipcRenderer.send('maximize'),
  close: () => ipcRenderer.send('close'),
  
  // Connection management
  testConnection: (host: string, port: number) => 
    ipcRenderer.invoke('test-connection', host, port),
  
  // Database operations
  listDatabases: (connectionId: string) => 
    ipcRenderer.invoke('list-databases', connectionId),
  
  // Query execution
  executeQuery: (connectionId: string, database: string, query: string) =>
    ipcRenderer.invoke('execute-query', connectionId, database, query),
})

// Type definitions for renderer
declare global {
  interface Window {
    electronAPI: {
      getAppVersion: () => Promise<string>
      getPlatform: () => Promise<string>
      minimize: () => void
      maximize: () => void
      close: () => void
      testConnection: (host: string, port: number) => Promise<boolean>
      listDatabases: (connectionId: string) => Promise<string[]>
      executeQuery: (connectionId: string, database: string, query: string) => Promise<any>
    }
  }
}
