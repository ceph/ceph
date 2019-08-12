import { $, browser, by, element, protractor } from 'protractor';
import { Helper } from '../helper.po';
import { PageHelper } from '../page-helper.po';

export class ConfigurationPageHelper extends PageHelper {
  pages = {
    index: '/#/configuration'
  };

  configClear(name) {
    // Clears out all the values in a config to reset before and after testing
    // Does not work for configs with checkbox only, possible future PR

    this.navigateTo();
    const valList = ['global', 'mon', 'mgr', 'osd', 'mds', 'client']; // Editable values

    // Selects config that we want to clear
    browser.wait(Helper.EC.elementToBeClickable(this.getTableCell(name)), Helper.TIMEOUT); // waits for config to be clickable
    this.getTableCell(name).click(); // click on the config to edit
    element(by.cssContainingText('button', 'Edit')).click(); // clicks button to edit

    for (const i of valList) {
      // Sends two backspaces to all values, clear() did not work in this instance, could be optimized more
      element(by.id(i)).sendKeys(protractor.Key.chord(protractor.Key.CONTROL, 'a'));
      element(by.id(i)).sendKeys(protractor.Key.BACK_SPACE);
    }
    // Clicks save button and checks that values are not present for the selected config
    element(by.cssContainingText('button', 'Save'))
      .click()
      .then(() => {
        browser
          .wait(Helper.EC.elementToBeClickable(this.getTableCell(name)), Helper.TIMEOUT)
          .then(() => {
            this.getTableCell(name)
              .click()
              .then(() => {
                // Clicks desired config
                browser.wait(
                  Helper.EC.visibilityOf($('.table.table-striped.table-bordered')), // Checks for visibility of details tab
                  Helper.TIMEOUT,
                  'config details did not appear'
                );
                for (const i of valList) {
                  // Waits until values are not present in the details table
                  browser.wait(
                    Helper.EC.not(
                      Helper.EC.textToBePresentInElement(
                        $('.table.table-striped.table-bordered'),
                        i + ':'
                      )
                    ),
                    Helper.TIMEOUT
                  );
                }
              });
          });
      });
  }

  edit(name, ...values: [string, string][]) {
    // Clicks the designated config, then inputs the values passed into the edit function.
    // Then checks if the edit is reflected in the config table. Takes in name of config and
    // a list of tuples of values the user wants edited, each tuple having the desired value along
    // with the number tehey want for that value. Ex: [global, '2'] is the global value with an input of 2

    this.navigateTo();
    // Selects config that we want to edit
    browser.wait(Helper.EC.elementToBeClickable(this.getTableCell(name)), Helper.TIMEOUT); // waits for config to be clickable
    this.getTableCell(name).click(); // click on the config to edit
    element(by.cssContainingText('button', 'Edit')).click(); // clicks button to edit

    expect(this.getBreadcrumbText()).toEqual('Edit');

    for (let i = 0, valtuple; (valtuple = values[i]); i++) {
      // Finds desired value based off given list
      element(by.id(valtuple[0])).sendKeys(valtuple[1]); // of values and inserts the given number for the value
    }

    // Clicks save button then waits until the desired config is visible, clicks it, then checks
    // that each desired value appears with the desired number
    element(by.cssContainingText('button', 'Save'))
      .click()
      .then(() => {
        this.navigateTo();
        browser.wait(Helper.EC.visibilityOf(this.getTableCell(name)), Helper.TIMEOUT).then(() => {
          // Checks for visibility of config in table
          this.getTableCell(name)
            .click()
            .then(() => {
              // Clicks config
              for (let i = 0, valtuple; (valtuple = values[i]); i++) {
                // iterates through list of values and
                browser.wait(
                  // checks if the value appears in details with the correct number attatched
                  Helper.EC.textToBePresentInElement(
                    $('.table.table-striped.table-bordered'),
                    valtuple[0] + ': ' + valtuple[1]
                  ),
                  Helper.TIMEOUT
                );
              }
            });
        });
      });
  }
}
