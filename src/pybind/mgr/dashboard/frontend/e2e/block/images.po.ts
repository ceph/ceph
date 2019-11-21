import { $, $$, browser, by, element } from 'protractor';
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
    await expect(element(by.id('pool')).getAttribute('class')).toContain('ng-valid'); // check if selected

    // Enter in the size of the image
    await element(by.id('size')).click();
    await element(by.id('size')).sendKeys(size);

    // Click the create button and wait for image to be made
    await element(by.cssContainingText('button', 'Create RBD')).click();
    return this.waitPresence(this.getFirstTableCellWithText(name));
  }

  async editImage(name, pool, newName, newSize) {
    const base_url = '/#/block/rbd/edit/';
    const editURL = base_url
      .concat(encodeURIComponent(pool))
      .concat('%2F')
      .concat(encodeURIComponent(name));
    await browser.get(editURL);

    await element(by.id('name')).click(); // click name box and send new name
    await element(by.id('name')).clear();
    await element(by.id('name')).sendKeys(newName);
    await element(by.id('size')).click();
    await element(by.id('size')).clear();
    await element(by.id('size')).sendKeys(newSize); // click the size box and send new size

    await element(by.cssContainingText('button', 'Edit RBD')).click();
    await this.navigateTo();
    await this.waitClickableAndClick(this.getFirstTableCellWithText(newName));
    await expect(
      element
        .all(by.css('.table.table-striped.table-bordered'))
        .first()
        .getText()
    ).toMatch(newSize);
  }

  // Selects RBD image and moves it to the trash, checks that it is present in the
  // trash table
  async moveToTrash(name) {
    await this.navigateTo();
    // wait for image to be created
    await this.waitTextNotPresent($$('.datatable-body').first(), '(Creating...)');
    await this.waitClickableAndClick(this.getFirstTableCellWithText(name));
    // click on the drop down and selects the move to trash option
    await $$('.table-actions button.dropdown-toggle')
      .first()
      .click();
    await $('li.move-to-trash').click();
    await this.waitVisibility(element(by.cssContainingText('button', 'Move Image')));
    await element(by.cssContainingText('button', 'Move Image')).click();
    await this.navigateTo();
    // Clicks trash tab
    await this.waitClickableAndClick(element(by.cssContainingText('.nav-link', 'Trash')));
    await this.waitPresence(this.getFirstTableCellWithText(name));
  }

  // Checks trash tab table for image and then restores it to the RBD Images table
  // (could change name if new name is given)
  async restoreImage(name, newName?: string) {
    await this.navigateTo();
    // clicks on trash tab
    await element(by.cssContainingText('.nav-link', 'Trash')).click();
    // wait for table to load
    await this.waitClickableAndClick(this.getFirstTableCellWithText(name));
    await element(by.cssContainingText('button', 'Restore')).click();
    // wait for pop-up to be visible (checks for title of pop-up)
    await this.waitVisibility(element(by.id('name')));
    // If a new name for the image is passed, it changes the name of the image
    if (newName !== undefined) {
      await element(by.id('name')).click(); // click name box and send new name
      await element(by.id('name')).clear();
      await element(by.id('name')).sendKeys(newName);
    }
    await element(by.cssContainingText('button', 'Restore Image')).click();
    await this.navigateTo();
    // clicks images tab
    await element(by.cssContainingText('.nav-link', 'Images')).click();
    await this.navigateTo();
    await this.waitPresence(this.getFirstTableCellWithText(newName));
  }

  // Enters trash tab and purges trash, thus emptying the trash table. Checks if
  // Image is still in the table.
  async purgeTrash(name, pool?: string) {
    await this.navigateTo();
    // clicks trash tab
    await element(by.cssContainingText('.nav-link', 'Trash')).click();
    await element(by.cssContainingText('button', 'Purge Trash')).click();
    // Check for visibility of modal container
    await this.waitVisibility(element(by.id('poolName')));
    // If purgeing a specific pool, selects that pool if given
    if (pool !== undefined) {
      const getPoolName = `[value="${pool}"]`;
      await element(by.id('poolName')).click();
      await element(by.cssContainingText('select[name=poolName] option', pool)).click();
      await $(getPoolName).click();
      await expect(element(by.id('poolName')).getAttribute('class')).toContain('ng-valid'); // check if pool is selected
    }
    await this.waitClickableAndClick(element(by.id('purgeFormButton')));
    // Wait for image to delete and check it is not present
    await this.waitStaleness(
      this.getFirstTableCellWithText(name),
      'Timed out waiting for image to be purged'
    );
    await expect(this.getFirstTableCellWithText(name).isPresent()).toBe(false);
  }
}
