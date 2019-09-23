import { $$, by, element } from 'protractor';
import { PageHelper } from '../page-helper.po';

export class ManagerModulesPageHelper extends PageHelper {
  pages = {
    index: '/#/mgr-modules'
  };

  // NOTABLE ISSUES: .clear() does not work on most text boxes, therefore using sendKeys
  // a Ctrl + 'a' BACK_SPACE is used.
  // The need to click the module repeatedly in the table is to ensure
  // that the values in the details tab updated. This fixed a bug I experienced.

  async editMgrModule(name: string, tuple: string[][]) {
    // Selects the Manager Module and then fills in the desired fields.
    // Doesn't check/uncheck boxes because it is not reflected in the details table.
    // DOES NOT WORK FOR ALL MGR MODULES, for example, Device health
    await this.navigateTo();
    await this.waitClickableAndClick(this.getFirstTableCellWithText(name));
    await element(by.cssContainingText('button', 'Edit')).click();

    for (const entry of tuple) {
      // Clears fields and adds edits
      await this.clearInput(element(by.id(entry[1])));
      await element(by.id(entry[1])).sendKeys(entry[0]);
    }

    await element(by.cssContainingText('button', 'Update')).click();
    // Checks if edits appear
    await this.navigateTo();
    await this.waitVisibility(this.getFirstTableCellWithText(name));
    await this.getFirstTableCellWithText(name).click();
    for (const entry of tuple) {
      await this.waitTextToBePresent($$('.datatable-body').last(), entry[0]);
    }

    // Clear mgr module of all edits made to it
    await this.navigateTo();
    await this.waitClickableAndClick(this.getFirstTableCellWithText(name));
    await element(by.cssContainingText('button', 'Edit')).click();

    // Clears the editable fields
    for (const entry of tuple) {
      await this.clearInput(element(by.id(entry[1])));
    }

    // Checks that clearing represents in details tab of module
    await element(by.cssContainingText('button', 'Update')).click();
    await this.navigateTo();
    await this.waitVisibility(this.getFirstTableCellWithText(name));
    await this.getFirstTableCellWithText(name).click();
    for (const entry of tuple) {
      await this.waitTextNotPresent($$('.datatable-body').last(), entry[0]);
    }
  }

  async editDevicehealth(
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
    await this.navigateTo();
    let devHealthArray: [string, string][];
    devHealthArray = [
      [threshhold, 'mark_out_threshold'],
      [pooln, 'pool_name'],
      [retention, 'retention_period'],
      [scrape, 'scrape_frequency'],
      [sleep, 'sleep_interval'],
      [warn, 'warn_threshold']
    ];

    await this.waitClickableAndClick(this.getFirstTableCellWithText('devicehealth'));
    await element(by.cssContainingText('button', 'Edit')).click();
    for (let i = 0, devHealthTuple; (devHealthTuple = devHealthArray[i]); i++) {
      if (devHealthTuple[0] !== undefined) {
        // Clears and inputs edits
        await this.clearInput(element(by.id(devHealthTuple[1])));
        await element(by.id(devHealthTuple[1])).sendKeys(devHealthTuple[0]);
      }
    }

    await element(by.cssContainingText('button', 'Update')).click();
    await this.navigateTo();
    await this.waitVisibility(this.getFirstTableCellWithText('devicehealth'));
    // Checks for visibility of devicehealth in table
    await this.getFirstTableCellWithText('devicehealth').click();
    for (let i = 0, devHealthTuple; (devHealthTuple = devHealthArray[i]); i++) {
      if (devHealthTuple[0] !== undefined) {
        await this.waitFn(async () => {
          // Repeatedly reclicks the module to check if edits has been done
          await element(by.cssContainingText('.datatable-body-cell-label', 'devicehealth')).click();
          return this.waitTextToBePresent($$('.datatable-body').last(), devHealthTuple[0]);
        });
      }
    }

    // Inputs old values into devicehealth fields. This manager module doesnt allow for updates
    // to be made when the values are cleared. Therefore, I restored them to their original values
    // (on my local run of ceph-dev, this is subject to change i would assume). I'd imagine there is a
    // better way of doing this.
    await this.navigateTo();
    await this.waitClickableAndClick(this.getFirstTableCellWithText('devicehealth')); // checks ansible
    await element(by.cssContainingText('button', 'Edit')).click();
    await this.clearInput(element(by.id('mark_out_threshold')));
    await element(by.id('mark_out_threshold')).sendKeys('2419200');

    await this.clearInput(element(by.id('pool_name')));
    await element(by.id('pool_name')).sendKeys('device_health_metrics');

    await this.clearInput(element(by.id('retention_period')));
    await element(by.id('retention_period')).sendKeys('15552000');

    await this.clearInput(element(by.id('scrape_frequency')));
    await element(by.id('scrape_frequency')).sendKeys('86400');

    await this.clearInput(element(by.id('sleep_interval')));
    await element(by.id('sleep_interval')).sendKeys('600');

    await this.clearInput(element(by.id('warn_threshold')));
    await element(by.id('warn_threshold')).sendKeys('7257600');

    // Checks that clearing represents in details tab of ansible
    await this.waitClickableAndClick(element(by.cssContainingText('button', 'Update')));
    await this.navigateTo();
    await this.waitVisibility(this.getFirstTableCellWithText('devicehealth'));
    await this.getFirstTableCellWithText('devicehealth').click();
    for (let i = 0, devHealthTuple; (devHealthTuple = devHealthArray[i]); i++) {
      if (devHealthTuple[0] !== undefined) {
        await this.waitFn(async () => {
          // Repeatedly reclicks the module to check if clearing has been done
          await element(by.cssContainingText('.datatable-body-cell-label', 'devicehealth')).click();
          return this.waitTextNotPresent($$('.datatable-body').last(), devHealthTuple[0]);
        });
      }
    }
  }
}
