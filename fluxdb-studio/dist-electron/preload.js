"use strict";
const electron = require("electron");
electron.contextBridge.exposeInMainWorld("electronAPI", {
  getAppVersion: () => electron.ipcRenderer.invoke("get-app-version"),
  getPlatform: () => electron.ipcRenderer.invoke("get-platform"),
  // Window controls
  minimize: () => electron.ipcRenderer.send("minimize"),
  maximize: () => electron.ipcRenderer.send("maximize"),
  close: () => electron.ipcRenderer.send("close"),
  // Connection management
  testConnection: (host, port) => electron.ipcRenderer.invoke("test-connection", host, port),
  // Database operations
  listDatabases: (connectionId) => electron.ipcRenderer.invoke("list-databases", connectionId),
  // Query execution
  executeQuery: (connectionId, database, query) => electron.ipcRenderer.invoke("execute-query", connectionId, database, query)
});
