import { $, $$, browser, ElementFinder } from 'protractor';

export class Helper {
  static EC = browser.ExpectedConditions;
  static TIMEOUT = 10000;

  /**
   * Checks if there are any errors on the browser
   *
   * @static
   * @memberof Helper
   */
  static checkConsole() {
    browser
      .manage()
      .logs()
      .get('browser')
      .then(function(browserLog) {
        browserLog = browserLog.filter((log) => {
          return log.level.value > 900; // SEVERE level
        });

        if (browserLog.length > 0) {
          console.log('\n log: ' + require('util').inspect(browserLog));
        }

        expect(browserLog.length).toEqual(0);
      });
  }

  static getBreadcrumb() {
    return $('.breadcrumb-item.active');
  }

  static getTabText(idx) {
    return $$('.nav.nav-tabs li')
      .get(idx)
      .getText();
  }

  static getTabsCount() {
    return $$('.nav.nav-tabs li').count();
  }

  static waitTextToBePresent(elem: ElementFinder, text: string, message?: string) {
    return browser.wait(Helper.EC.textToBePresentInElement(elem, text), Helper.TIMEOUT, message);
  }
}
