const fs = require('fs')
var branch = new String();

// Read the contents of the ceph_release file to retrieve
// the branch
fs.readFile('../../../../ceph_release', (_, data) => {
    branch = data.toString().split('\n')[1];
});

module.exports = {
  appName: 'Ceph Dashboard',
  apiKey: process.env.APPLITOOLS_API_KEY,
  browser: [
    { width: 1920, height: 1080, name: 'chrome' },
    { width: 1920, height: 1080, name: 'firefox' },
    { width: 800, height: 600, name: 'chrome' },
    { width: 800, height: 600, name: 'firefox' }
  ],
  showLogs: false,
  saveDebugData: true,
  failCypressOnDiff: true,
  concurrency: 4,
  baselineBranchName: branch
};
