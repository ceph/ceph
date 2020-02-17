// Protractor configuration file, see link for more information
// https://github.com/angular/protractor/blob/master/lib/config.ts

const { SpecReporter } = require('jasmine-spec-reporter');
let failFast = require("protractor-fail-fast");

const config = {
  SELENIUM_PROMISE_MANAGER: false,
  allScriptsTimeout: 11000,
  implicitWaitTimeout: 9000,
  suites: {
    block: './e2e/block/*.e2e-spec.ts',
    cluster: './e2e/cluster/*.e2e-spec.ts',
    filesystems: './e2e/filesystems/*.e2e-spec.ts',
    nfs: './e2e/nfs/*.e2e-spec.ts',
    pools: './e2e/pools/*.e2e-spec.ts',
    rgw: './e2e/rgw/*.e2e-spec.ts',
    ui: './e2e/ui/*.e2e-spec.ts'
  },
  capabilities: {
    browserName: 'chrome',
    chromeOptions: {
      args: ['--no-sandbox', '--headless', '--window-size=1920x1080']
    },
    acceptInsecureCerts : true
  },
  directConnect: true,
  baseUrl: process.env.BASE_URL || 'http://localhost:4200/',
  framework: 'jasmine',
  jasmineNodeOpts: {
    showColors: true,
    defaultTimeoutInterval: 300000,
    print: function() {}
  },
  params: {
    login: {
      user: 'admin',
      password: 'admin'
    }
  },

  plugins: [
    {
      package: 'protractor-screenshoter-plugin',
      screenshotPath: '.protractor-report',
      screenshotOnExpect: 'failure',
      screenshotOnSpec: 'none',
      withLogs: true,
      writeReportFreq: 'asap',
      imageToAscii: 'none',
      clearFoldersBeforeTest: true
    },
    failFast.init()
  ],
  afterLaunch: function() {
    failFast.clean();
  }
};

config.onPrepare = async () => {
  await browser.manage().timeouts().implicitlyWait(config.implicitWaitTimeout);

  require('ts-node').register({
    project: 'e2e/tsconfig.e2e.json'
  });
  jasmine.getEnv().addReporter(new SpecReporter({ spec: { displayStacktrace: true, displayDuration: true } }));

  await browser.get('/#/login');

  await browser.driver.findElement(by.name('username')).clear();
  await browser.driver.findElement(by.name('username')).sendKeys(browser.params.login.user);
  await browser.driver.findElement(by.name('password')).clear();
  await browser.driver.findElement(by.name('password')).sendKeys(browser.params.login.password);

  await browser.driver.findElement(by.css('input[type="submit"]')).click();
};

exports.config = config;
