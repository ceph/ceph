import { $, $$, browser, by, element } from 'protractor';
import { Helper } from '../helper.po';
import { PageHelper } from '../page-helper.po';

export class ImagesPageHelper extends PageHelper {
  pages = {
    index: '/#/block/rbd',
    create: '/#/block/rbd/create'
  };

  // Creates a block image and fills in the name, pool, and size fields. Then checks
  // if the image is present in the Images table.
  async createImage(name, pool, size) {
    await this.navigateTo('create');

    // Need the string '[value="<pool>"]' to find the pool in the dropdown menu
    const getPoolName = `[value="${pool}"]`;

    await element(by.id('name')).sendKeys(name); // Enter in image name

    // Select image pool
    await element(by.id('pool')).click();
    await element(by.cssContainingText('select[name=pool] option', pool)).click();
    await $(getPoolName).click();
    expect(await element(by.id('pool')).getAttribute('class')).toContain('ng-valid'); // check if selected

    // Enter in the size of the image
    await element(by.id('size')).click();
    await element(by.id('size')).sendKeys(size);

    // Click the create button and wait for image to be made
    await element(by.cssContainingText('button', 'Create RBD')).click();
    await browser.wait(Helper.EC.presenceOf(this.getTableCell(name)), Helper.TIMEOUT);
  }

  async editImage(name, pool, newName, newSize) {
    const base_url = '/#/block/rbd/edit/';
    const editURL = base_url
      .concat(pool)
      .concat('/')
      .concat(name);
    await browser.get(editURL);

    await element(by.id('name')).click(); // click name box and send new name
    await element(by.id('name')).clear();
    await element(by.id('name')).sendKeys(newName);
    await element(by.id('size')).click();
    await element(by.id('size')).clear();
    await element(by.id('size')).sendKeys(newSize); // click the size box and send new size

    await element(by.cssContainingText('button', 'Edit RBD')).click();
    await this.navigateTo();
    await browser.wait(Helper.EC.elementToBeClickable(this.getTableCell(newName)), Helper.TIMEOUT);
    // click edit button and wait to make sure new owner is present in table
    await this.getTableCell(newName).click();
    expect(
      await element
        .all(by.css('.table.table-striped.table-bordered'))
        .first()
        .getText()
    ).toMatch(newSize);
  }

  async deleteImage(name) {
    await this.navigateTo();

    // wait for table to load
    await browser.wait(Helper.EC.elementToBeClickable(this.getTableCell(name)), Helper.TIMEOUT);
    await this.getTableCell(name).click(); // click on the image you want to delete in the table
    await $$('.table-actions button.dropdown-toggle')
      .first()
      .click(); // click toggle menu
    await $('li.delete.ng-star-inserted').click(); // click delete
    // wait for pop-up to be visible (checks for title of pop-up)
    await browser.wait(Helper.EC.visibilityOf($('.modal-body')), Helper.TIMEOUT);
    await this.clickCheckbox($('.custom-control-label'));
    await element(by.cssContainingText('button', 'Delete RBD')).click();
    await browser.wait(Helper.EC.stalenessOf(this.getTableCell(name)), Helper.TIMEOUT);
  }
}
