module.exports = {
  appName: 'Ceph Dashboard',
  apiKey: process.env.APPLITOOLS_API_KEY,
  browser: [
    { width: 1920, height: 1080, name: 'chrome' },
    { width: 1920, height: 1080, name: 'firefox' },
    { width: 800, height: 600, name: 'chrome' },
    { width: 800, height: 600, name: 'firefox' }
  ],
  showLogs: true,
  saveDebugData: true,
  failCypressOnDiff: true,
  concurrency: 4
};
