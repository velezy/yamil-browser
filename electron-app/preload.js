const { contextBridge } = require('electron')

contextBridge.exposeInMainWorld('YAMIL_CONFIG', {
  AI_ENDPOINT: process.env.AI_ENDPOINT || 'http://localhost:9080/api/v1/builder-orchestra/browser-chat',
  APP_TITLE:   process.env.APP_TITLE   || 'YAMIL Browser',
  START_URL:   process.env.START_URL   || 'https://yamil-ai.com',
})
