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
  createImage(name, pool, size) {
    this.navigateTo('create');

    // Need the string '[value="<pool>"]' to find the pool in the dropdown menu
    const getPoolName = `[value="${pool}"]`;

    element(by.id('name')).sendKeys(name); // Enter in image name

    // Select image pool
    element(by.id('pool')).click();
    element(by.cssContainingText('select[name=pool] option', pool)).click();
    $(getPoolName).click();
    expect(element(by.id('pool')).getAttribute('class')).toContain('ng-valid'); // check if selected

    // Enter in the size of the image
    element(by.id('size')).click();
    element(by.id('size')).sendKeys(size);

    // Click the create button and wait for image to be made
    element(by.cssContainingText('button', 'Create RBD'))
      .click()
      .then(() => {
        browser.wait(Helper.EC.presenceOf(this.getTableCell(name)), Helper.TIMEOUT);
      });
  }

  editImage(name, pool, newName, newSize) {
    const base_url = '/#/block/rbd/edit/';
    const editURL = base_url
      .concat(pool)
      .concat('/')
      .concat(name);
    browser.get(editURL);

    element(by.id('name')).click(); // click name box and send new name
    element(by.id('name')).clear();
    element(by.id('name')).sendKeys(newName);
    element(by.id('size')).click();
    element(by.id('size')).clear();
    element(by.id('size')).sendKeys(newSize); // click the size box and send new size

    element(by.cssContainingText('button', 'Edit RBD'))
      .click()
      .then(() => {
        this.navigateTo();
        browser
          .wait(Helper.EC.elementToBeClickable(this.getTableCell(newName)), Helper.TIMEOUT)
          .then(() => {
            this.getTableCell(newName).click();
            expect(
              element
                .all(by.css('.table.table-striped.table-bordered'))
                .first()
                .getText()
            ).toMatch(newSize);
          }); // click edit button and wait to make sure new owner is present in table
      });
  }

  deleteImage(name) {
    this.navigateTo();

    // wait for table to load
    browser.wait(Helper.EC.elementToBeClickable(this.getTableCell(name)), Helper.TIMEOUT);
    this.getTableCell(name).click(); // click on the image you want to delete in the table
    $$('.table-actions button.dropdown-toggle')
      .first()
      .click(); // click toggle menu
    $('li.delete.ng-star-inserted').click(); // click delete
    // wait for pop-up to be visible (checks for title of pop-up)
    browser.wait(Helper.EC.visibilityOf($('.modal-body')), Helper.TIMEOUT).then(() => {
      $('.custom-control-label').click(); // click confirmation checkbox
      element(by.cssContainingText('button', 'Delete RBD'))
        .click()
        .then(() => {
          browser.wait(Helper.EC.stalenessOf(this.getTableCell(name)), Helper.TIMEOUT);
        });
    });
  }
}
