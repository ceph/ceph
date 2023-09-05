const fs = require('fs');

// Read the contents of the ceph_release file to retrieve
// the branch
const cephRelease = fs.readFileSync('../../../../ceph_release', 'utf8').split('\n');
const branch = cephRelease[2] === 'dev' ? 'main' : cephRelease[1];
module.exports = {
  appName: 'Ceph Dashboard',
  batchId: process.env.APPLITOOLS_BATCH_ID, 
  apiKey: process.env.APPLITOOLS_API_KEY,
  browser: [
    { width: 1920, height: 1080, name: 'chrome' },
    { width: 1920, height: 1080, name: 'firefox' }
  ],
  showLogs: false,
  saveDebugData: true,
  failCypressOnDiff: true,
  concurrency: 4,
  baselineBranchName: branch
};
