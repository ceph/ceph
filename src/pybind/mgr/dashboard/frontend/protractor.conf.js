// Protractor configuration file, see link for more information
// https://github.com/angular/protractor/blob/master/lib/config.ts

const { SpecReporter } = require('jasmine-spec-reporter');

exports.config = {
  allScriptsTimeout: 11000,
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
  baseUrl: 'http://localhost:4200/',
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

  onPrepare() {
    browser.manage().timeouts().implicitlyWait(360000);

    require('ts-node').register({
      project: 'e2e/tsconfig.e2e.json'
    });
    jasmine.getEnv().addReporter(new SpecReporter({ spec: { displayStacktrace: true } }));

    browser.get('/#/login');

    browser.driver.findElement(by.name('username')).clear();
    browser.driver.findElement(by.name('username')).sendKeys(browser.params.login.user);
    browser.driver.findElement(by.name('password')).clear();
    browser.driver.findElement(by.name('password')).sendKeys(browser.params.login.password);

    browser.driver.findElement(by.css('input[type="submit"]')).click();

    return global.browser.getProcessedConfig().then(function(config) {
      // Login takes some time, so wait until it's done.
      // For the test app's login, we know it's done when it redirects to
      // dashboard.
      return browser.driver.wait(function() {
        return browser.driver.getCurrentUrl().then(function(url) {
          return /dashboard/.test(url);
        });
      });
    });
  }
};
