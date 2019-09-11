import { $, by, element, protractor } from 'protractor';
import { PageHelper } from '../page-helper.po';

export class ConfigurationPageHelper extends PageHelper {
  pages = {
    index: '/#/configuration'
  };

  async configClear(name) {
    // Clears out all the values in a config to reset before and after testing
    // Does not work for configs with checkbox only, possible future PR

    await this.navigateTo();
    const valList = ['global', 'mon', 'mgr', 'osd', 'mds', 'client']; // Editable values

    // Enter config setting name into filter box
    await $('input.form-control.ng-valid').clear();
    await $('input.form-control.ng-valid').sendKeys(name);

    // Selects config that we want to clear
    await this.waitClickableAndClick(this.getFirstTableCellWithText(name)); // waits for config to be clickable and click
    await element(by.cssContainingText('button', 'Edit')).click(); // clicks button to edit

    for (const i of valList) {
      // Sends two backspaces to all values, clear() did not work in this instance, could be optimized more
      await element(by.id(i)).sendKeys(protractor.Key.chord(protractor.Key.CONTROL, 'a'));
      await element(by.id(i)).sendKeys(protractor.Key.BACK_SPACE);
    }
    // Clicks save button and checks that values are not present for the selected config
    await element(by.cssContainingText('button', 'Save')).click();

    // Enter config setting name into filter box
    await $('input.form-control.ng-valid').clear();
    await $('input.form-control.ng-valid').sendKeys(name);

    await this.waitClickableAndClick(this.getFirstTableCellWithText(name));
    // Clicks desired config
    await this.waitVisibility(
      $('.table.table-striped.table-bordered'), // Checks for visibility of details tab
      'config details did not appear'
    );
    for (const i of valList) {
      // Waits until values are not present in the details table
      await this.waitTextNotPresent($('.table.table-striped.table-bordered'), i + ':');
    }
  }

  async edit(name, ...values: [string, string][]) {
    // Clicks the designated config, then inputs the values passed into the edit function.
    // Then checks if the edit is reflected in the config table. Takes in name of config and
    // a list of tuples of values the user wants edited, each tuple having the desired value along
    // with the number tehey want for that value. Ex: [global, '2'] is the global value with an input of 2
    await this.navigateTo();

    // Enter config setting name into filter box
    await $('input.form-control.ng-valid').clear();
    await $('input.form-control.ng-valid').sendKeys(name);

    // Selects config that we want to edit
    await this.waitClickableAndClick(this.getFirstTableCellWithText(name)); // waits for config to be clickable and click
    await element(by.cssContainingText('button', 'Edit')).click(); // clicks button to edit

    await this.waitTextToBePresent(this.getBreadcrumb(), 'Edit');

    for (let i = 0, valtuple; (valtuple = values[i]); i++) {
      // Finds desired value based off given list
      await element(by.id(valtuple[0])).sendKeys(valtuple[1]); // of values and inserts the given number for the value
    }

    // Clicks save button then waits until the desired config is visible, clicks it, then checks
    // that each desired value appears with the desired number
    await element(by.cssContainingText('button', 'Save')).click();
    await this.navigateTo();

    // Enter config setting name into filter box
    await $('input.form-control.ng-valid').clear();
    await $('input.form-control.ng-valid').sendKeys(name);

    await this.waitVisibility(this.getFirstTableCellWithText(name));
    // Checks for visibility of config in table
    await this.getFirstTableCellWithText(name).click();
    // Clicks config
    for (let i = 0, valtuple; (valtuple = values[i]); i++) {
      // iterates through list of values and
      await this.waitTextToBePresent(
        // checks if the value appears in details with the correct number attatched
        $('.table.table-striped.table-bordered'),
        valtuple[0] + ': ' + valtuple[1]
      );
    }
  }
}
