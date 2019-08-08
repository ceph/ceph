import { $, by, element } from 'protractor';
import { PageHelper } from '../page-helper.po';

/** Class holding silences page functions **/
export class SilencesPageHelper extends PageHelper {
  pages = {
    index: '/#/silence',
    create: '/#/silence/create'
  };

  /**
   * Create a silence with given creator, comment, matcher name and matcher value
   */
  async create(creator, comment, matcher_name, matcher_value) {
    await this.navigateTo('create');

    await this.waitTextToBePresent(this.getBreadcrumb(), 'Create');

    // enter silence creator
    await element(by.id('created-by')).clear();
    await element(by.id('created-by')).sendKeys(creator);

    // enter silence comment
    await element(by.id('comment')).clear();
    await element(by.id('comment')).sendKeys(comment);

    const addMatcherButton = element(by.cssContainingText('button', 'Add matcher'));
    await addMatcherButton.click();
    // select matcher name
    const nameField = element(by.id('name'));
    await nameField.click();
    await expect(nameField.all(by.css('option')).getText()).toMatch(
      matcher_name,
      'Invalid matcher name'
    );
    await nameField.element(by.cssContainingText('option', matcher_name)).click();

    // enter matcher value
    const valueField = element(by.id('value'));
    await valueField.click();
    await valueField.sendKeys(matcher_value);

    const addButton = element.all(by.cssContainingText('button', 'Add')).last();
    await addButton.click();
    const createButton = element(by.cssContainingText('button', 'Create Silence'));
    // wait for modal container where you select Matcher to dissapear
    await this.waitStaleness(element(by.cssContainingText('.modal-dialog', 'Matcher')));
    await createButton.click();
    // wait for browser to return to silences page and table to be visible
    await this.waitPresence(this.getDataTables().first(), 'Timed out waiting for silence creation');
    // Order silences so most recently updated one is on top. Requires clicking 'Updated' twice
    await element(
      by.cssContainingText('.datatable-header-cell-label.draggable', 'Updated')
    ).click();
    await element(
      by.cssContainingText('.datatable-header-cell-label.draggable', 'Updated')
    ).click();
    // Opens new silence's details table by clicking on silence then check "comment"
    // row of details table for silence for the correct comment to verify its existence
    await this.getFirstTableCellWithText(creator).click();
    await expect(element(by.cssContainingText('.datatable-body-row', 'comment')).getText()).toMatch(
      comment
    );
  }

  /**
   * Function to expire a silence.
   * Expire is currently limited to expiring the most recently updated silence with the given creator name
   */
  async expire(creator) {
    await this.navigateTo();

    // filter silence table so only silences with given creator are present
    const filterTableField = $('input.form-control.ng-valid');
    await filterTableField.clear();
    await filterTableField.sendKeys(creator);

    // Order silences so most recently updated one is on top, requires clicking "Updated" twice
    await element(
      by.cssContainingText('.datatable-header-cell-label.draggable', 'Updated')
    ).click();
    await element(
      by.cssContainingText('.datatable-header-cell-label.draggable', 'Updated')
    ).click();

    // Opens new silence's details table by clicking on silence then check "comment"
    // row of details table for silence for the correct comment to verify its existence
    await this.getFirstTableCellWithText(creator).click();

    await expect(element(by.cssContainingText('.datatable-body-row', 'state')).getText()).toMatch(
      'active',
      'Error: Silence was already expired'
    );

    await $('.table-actions button.dropdown-toggle').click(); // click toggle menu
    await $('li.expire a').click(); // click expire
    await this.waitVisibility($('.custom-control-label'));
    await $('.custom-control-label').click();
    await element(by.cssContainingText('button', 'Expire Silence')).click();
    await this.waitStaleness(element(by.cssContainingText('.modal-dialog', 'Expire Silence')));
    await this.getFirstTableCellWithText(creator).click();
    await expect(element(by.cssContainingText('.datatable-body-row', 'state')).getText()).toMatch(
      'expired',
      'Error: Silence not expired'
    );
  }
}
