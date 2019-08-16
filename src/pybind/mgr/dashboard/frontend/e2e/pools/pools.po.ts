import { $, browser, by, element, protractor } from 'protractor';
import { Helper } from '../helper.po';
import { PageHelper } from '../page-helper.po';

const EC = protractor.ExpectedConditions;
const pages = {
  index: '/#/pool',
  create: '/#/pool/create'
};

export class PoolPageHelper extends PageHelper {
  pages = pages;

  private isPowerOf2(n: number): boolean {
    // tslint:disable-next-line: no-bitwise
    return (n & (n - 1)) === 0;
  }

  @PageHelper.restrictTo(pages.index)
  async exist(name: string, oughtToBePresent = true) {
    const tableCell = await this.getTableCellByContent(name);
    const waitFn = oughtToBePresent ? EC.visibilityOf(tableCell) : EC.invisibilityOf(tableCell);
    try {
      await browser.wait(waitFn, Helper.TIMEOUT);
    } catch (e) {
      const visibility = oughtToBePresent ? 'invisible' : 'visible';
      const msg = `Pool "${name}" is ${visibility}, but should not be. Waiting for a change timed out`;
      return Promise.reject(msg);
    }
    return Promise.resolve();
  }

  @PageHelper.restrictTo(pages.create)
  async create(name: string, placement_groups: number, ...apps: string[]): Promise<any> {
    const nameInput = $('input[name=name]');
    await nameInput.clear();
    if (!this.isPowerOf2(placement_groups)) {
      return Promise.reject(`Placement groups ${placement_groups} are not a power of 2`);
    }
    await nameInput.sendKeys(name);
    await element(by.cssContainingText('select[name=poolType] option', 'replicated')).click();

    expect(await element(by.css('select[name=poolType] option:checked')).getText()).toBe(
      ' replicated '
    );
    await $('input[name=pgNum]').sendKeys(
      protractor.Key.CONTROL,
      'a',
      protractor.Key.NULL,
      placement_groups
    );
    this.setApplications(apps);
    await element(by.css('cd-submit-button')).click();

    return Promise.resolve();
  }

  edit_pool_pg(name: string, new_pg: number): promise.Promise<any> {
    if (!this.isPowerOf2(new_pg)) {
      return Promise.reject(`Placement groups ${new_pg} are not a power of 2`);
    }
    return this.getTableCellByContent(name).then((elem) => {
      elem.click(); // select pool from the table
      element(by.cssContainingText('button', 'Edit')).click(); // click edit button
      expect(this.getBreadcrumbText()).toEqual('Edit'); // verify we are now on edit page
      $('input[name=pgNum]')
        .sendKeys(protractor.Key.CONTROL, 'a', protractor.Key.NULL, new_pg)
        .then(() => {
          element(by.css('cd-submit-button')).click();
          const str = `${new_pg} active+clean`;
          browser
            .wait(
              EC.visibilityOf(this.getTableRow(name)),
              Helper.TIMEOUT,
              'Timed out waiting for table row to load'
            )
            .then(() => {
              return browser.wait(
                EC.textToBePresentInElement(this.getTableRow(name), str),
                Helper.TIMEOUT,
                'Timed out waiting for placement group to be updated'
              );
            });
        });
    });
  }

  private setApplications(apps: string[]) {
    if (!apps || apps.length === 0) {
      return;
    }
    element(by.css('.float-left.mr-2.select-menu-edit'))
      .click()
      .then(() => {
        browser
          .wait(
            Helper.EC.visibilityOf(element(by.css('.popover-content.popover-body'))),
            Helper.TIMEOUT
          )
          .then(() =>
            apps.forEach((app) =>
              element(by.cssContainingText('.select-menu-item-content', app)).click()
            )
          );
      });
  }

  @PageHelper.restrictTo(pages.index)
  async delete(name: string): Promise<any> {
    const tableCell = await this.getTableCellByContent(name);
    await tableCell.click();
    await $('.table-actions button.dropdown-toggle').click(); // open submenu
    await $('li.delete a').click(); // click on "delete" menu item
    const confirmationInput = () => $('#confirmation');
    await browser.wait(() => EC.visibilityOf(confirmationInput()), Helper.TIMEOUT);
    await this.clickCheckbox(confirmationInput());
    await element(by.cssContainingText('button', 'Delete Pool')).click(); // Click Delete item

    return Promise.resolve();
  }
}
