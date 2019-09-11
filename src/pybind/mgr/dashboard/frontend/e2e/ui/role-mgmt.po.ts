import { by, element } from 'protractor';
import { PageHelper } from '../page-helper.po';

export class RoleMgmtPageHelper extends PageHelper {
  pages = {
    index: '/#/user-management/roles',
    create: '/#/user-management/roles/create'
  };

  async create(name, description): Promise<void> {
    await this.navigateTo('create');

    // fill in fields
    await element(by.id('name')).sendKeys(name);
    await element(by.id('description')).sendKeys(description);

    // Click the create button and wait for role to be made
    const createButton = element(by.cssContainingText('button', 'Create Role'));
    await createButton.click();

    await this.waitPresence(this.getFirstTableCellWithText(name));
  }

  async edit(name, description): Promise<void> {
    await this.navigateTo();

    await this.getFirstTableCellWithText(name).click(); // select role from table
    await element(by.cssContainingText('button', 'Edit')).click(); // click button to move to edit page

    // fill in fields with new values
    await element(by.id('description')).clear();
    await element(by.id('description')).sendKeys(description);

    // Click the edit button and check new values are present in table
    const editButton = element(by.cssContainingText('button', 'Edit Role'));
    await editButton.click();

    await this.waitPresence(this.getFirstTableCellWithText(name));
    await this.waitPresence(this.getFirstTableCellWithText(description));
  }
}
