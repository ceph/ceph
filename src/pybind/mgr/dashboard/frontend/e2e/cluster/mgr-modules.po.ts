import { $$, browser, by, element } from 'protractor';
import { Helper } from '../helper.po';
import { PageHelper } from '../page-helper.po';

export class ManagerModulesPageHelper extends PageHelper {
  pages = {
    index: '/#/mgr-modules'
  };

  // NOTABLE ISSUES: .clear() does not work on most text boxes, therefore using sendKeys
  // a Ctrl + 'a' BACK_SPACE is used.
  // The need to click the module repeatedly in the table is to ensure
  // that the values in the details tab updated. This fixed a bug I experienced.

  editMgrModule(name: string, tuple: string[][]) {
    // Selects the Manager Module and then fills in the desired fields.
    // Doesn't check/uncheck boxes because it is not reflected in the details table.
    // DOES NOT WORK FOR ALL MGR MODULES, for example, Device health
    this.navigateTo();
    browser.wait(Helper.EC.elementToBeClickable(this.getTableCell(name)), Helper.TIMEOUT);
    this.getTableCell(name).click();
    element(by.cssContainingText('button', 'Edit')).click();

    for (const entry of tuple) {
      // Clears fields and adds edits
      this.inputClear(element(by.id(entry[1])));
      element(by.id(entry[1])).sendKeys(entry[0]);
    }

    element(by.cssContainingText('button', 'Update'))
      .click()
      .then(() => {
        // Checks if edits appear
        this.navigateTo();
        browser.wait(Helper.EC.visibilityOf(this.getTableCell(name)), Helper.TIMEOUT).then(() => {
          this.getTableCell(name)
            .click()
            .then(() => {
              for (const entry of tuple) {
                browser.wait(
                  Helper.EC.textToBePresentInElement($$('.datatable-body').last(), entry[0]),
                  Helper.TIMEOUT
                );
              }
            });
        });
      });

    // Clear mgr module of all edits made to it
    this.navigateTo();
    browser.wait(Helper.EC.elementToBeClickable(this.getTableCell(name)), Helper.TIMEOUT);
    this.getTableCell(name).click();
    element(by.cssContainingText('button', 'Edit')).click();

    // Clears the editable fields
    for (const entry of tuple) {
      this.inputClear(element(by.id(entry[1])));
    }

    // Checks that clearing represents in details tab of module
    element(by.cssContainingText('button', 'Update'))
      .click()
      .then(() => {
        this.navigateTo();
        browser.wait(Helper.EC.visibilityOf(this.getTableCell(name)), Helper.TIMEOUT).then(() => {
          this.getTableCell(name)
            .click()
            .then(() => {
              for (const entry of tuple) {
                browser.wait(
                  Helper.EC.not(
                    Helper.EC.textToBePresentInElement($$('.datatable-body').last(), entry[0])
                  ),
                  Helper.TIMEOUT
                );
              }
            });
        });
      });
  }

  editDevicehealth(
    threshhold?: string,
    pooln?: string,
    retention?: string,
    scrape?: string,
    sleep?: string,
    warn?: string
  ) {
    // Isn't called by editMgrModule since clearing doesn't work.
    // Selects the Devicehealth manager module, then fills in the desired fields, including all fields except
    // checkboxes. Clicking checkboxes has been a notable issue in Protractor, therefore they were omitted in this
    // version of the tests. Could be added in a future PR. Then checks if these edits appear in the details table.
    this.navigateTo();
    let devHealthArray: [string, string][];
    devHealthArray = [
      [threshhold, 'mark_out_threshold'],
      [pooln, 'pool_name'],
      [retention, 'retention_period'],
      [scrape, 'scrape_frequency'],
      [sleep, 'sleep_interval'],
      [warn, 'warn_threshold']
    ];

    browser.wait(Helper.EC.elementToBeClickable(this.getTableCell('devicehealth')), Helper.TIMEOUT);
    this.getTableCell('devicehealth').click();
    element(by.cssContainingText('button', 'Edit'))
      .click()
      .then(() => {
        for (let i = 0, devHealthTuple; (devHealthTuple = devHealthArray[i]); i++) {
          if (devHealthTuple[0] !== undefined) {
            // Clears and inputs edits
            this.inputClear(element(by.id(devHealthTuple[1])));
            element(by.id(devHealthTuple[1])).sendKeys(devHealthTuple[0]);
          }
        }
      });

    element(by.cssContainingText('button', 'Update'))
      .click()
      .then(() => {
        this.navigateTo();
        browser
          .wait(Helper.EC.visibilityOf(this.getTableCell('devicehealth')), Helper.TIMEOUT)
          .then(() => {
            // Checks for visibility of devicehealth in table
            this.getTableCell('devicehealth')
              .click()
              .then(() => {
                for (let i = 0, devHealthTuple; (devHealthTuple = devHealthArray[i]); i++) {
                  if (devHealthTuple[0] !== undefined) {
                    browser.wait(function() {
                      // Repeatedly reclicks the module to check if edits has been done
                      element(
                        by.cssContainingText('.datatable-body-cell-label', 'devicehealth')
                      ).click();
                      return Helper.EC.textToBePresentInElement(
                        $$('.datatable-body').last(),
                        devHealthTuple[0]
                      );
                    }, Helper.TIMEOUT);
                  }
                }
              });
          });
      });

    // Inputs old values into devicehealth fields. This manager module doesnt allow for updates
    // to be made when the values are cleared. Therefore, I restored them to their original values
    // (on my local run of ceph-dev, this is subject to change i would assume). I'd imagine there is a
    // better way of doing this.
    this.navigateTo();
    browser.wait(Helper.EC.elementToBeClickable(this.getTableCell('devicehealth')), Helper.TIMEOUT); // checks ansible
    this.getTableCell('devicehealth').click();
    element(by.cssContainingText('button', 'Edit'))
      .click()
      .then(() => {
        this.inputClear(element(by.id('mark_out_threshold')));
        element(by.id('mark_out_threshold')).sendKeys('2419200');

        this.inputClear(element(by.id('pool_name')));
        element(by.id('pool_name')).sendKeys('device_health_metrics');

        this.inputClear(element(by.id('retention_period')));
        element(by.id('retention_period')).sendKeys('15552000');

        this.inputClear(element(by.id('scrape_frequency')));
        element(by.id('scrape_frequency')).sendKeys('86400');

        this.inputClear(element(by.id('sleep_interval')));
        element(by.id('sleep_interval')).sendKeys('600');

        this.inputClear(element(by.id('warn_threshold')));
        element(by.id('warn_threshold')).sendKeys('7257600');
      });

    // Checks that clearing represents in details tab of ansible
    browser
      .wait(Helper.EC.elementToBeClickable(element(by.cssContainingText('button', 'Update'))))
      .then(() => {
        element(by.cssContainingText('button', 'Update'))
          .click()
          .then(() => {
            this.navigateTo();
            browser
              .wait(Helper.EC.visibilityOf(this.getTableCell('devicehealth')), Helper.TIMEOUT)
              .then(() => {
                this.getTableCell('devicehealth')
                  .click()
                  .then(() => {
                    for (let i = 0, devHealthTuple; (devHealthTuple = devHealthArray[i]); i++) {
                      if (devHealthTuple[0] !== undefined) {
                        browser.wait(function() {
                          // Repeatedly reclicks the module to check if clearing has been done
                          element(
                            by.cssContainingText('.datatable-body-cell-label', 'devicehealth')
                          ).click();
                          return Helper.EC.not(
                            Helper.EC.textToBePresentInElement(
                              $$('.datatable-body').last(),
                              devHealthTuple[0]
                            )
                          );
                        }, Helper.TIMEOUT);
                      }
                    }
                  });
              });
          });
      });
  }
}
