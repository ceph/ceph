// Protractor configuration file, see link for more information
// https://github.com/angular/protractor/blob/master/lib/config.ts

const { SpecReporter } = require('jasmine-spec-reporter');

const config = {
  SELENIUM_PROMISE_MANAGER: false,
  allScriptsTimeout: 11000,
  implicitWaitTimeout: 360000,
  specs: [
    './e2e/**/*.e2e-spec.ts'
  ],
  capabilities: {
    'browserName': 'chrome',
    chromeOptions: {
      args: ['--no-sandbox', '--headless', '--window-size=1920x1080']
    }
  },
  directConnect: true,
  baseUrl: process.env.BASE_URL || 'http://localhost:4200/',
  framework: 'jasmine',
  jasmineNodeOpts: {
    showColors: true,
    defaultTimeoutInterval: 30000,
    print: function() {}
  },
  params: {
    login: {
      user: 'admin',
      password: 'admin'
    }
  },

  plugins: [{
      package: 'protractor-screenshoter-plugin',
      screenshotPath: '.protractor-report',
      screenshotOnExpect: 'failure',
      screenshotOnSpec: 'none',
      withLogs: true,
      writeReportFreq: 'asap',
      imageToAscii: 'none',
      clearFoldersBeforeTest: true
    }],
};

config.onPrepare = async () => {
    browser.manage().timeouts().implicitlyWait(exports.config.implicitWaitTimeout);

    require('ts-node').register({
      project: 'e2e/tsconfig.e2e.json'
    });
    jasmine.getEnv().addReporter(new SpecReporter({ spec: { displayStacktrace: true } }));

    await browser.get('/#/login');

    await browser.driver.findElement(by.name('username')).clear();
    await browser.driver.findElement(by.name('username')).sendKeys(browser.params.login.user);
    await browser.driver.findElement(by.name('password')).clear();
    await browser.driver.findElement(by.name('password')).sendKeys(browser.params.login.password);

    await browser.driver.findElement(by.css('input[type="submit"]')).click();

    global.browser.getProcessedConfig().then(async (config) => {
      // Login takes some time, so wait until it's done.
      // For the test app's login, we know it's done when it redirects to
      // dashboard.
      return await browser.driver.wait(async () => {
        return await browser.driver.getCurrentUrl().then((url) => {
          return /dashboard/.test(url);
        });
      });
    });
  }

exports.config = config;
