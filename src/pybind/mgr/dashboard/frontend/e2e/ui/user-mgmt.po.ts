import { $, by, element } from 'protractor';
import { PageHelper } from '../page-helper.po';

export class UserMgmtPageHelper extends PageHelper {
  pages = {
    index: '/#/user-management/users',
    create: '/#/user-management/users/create'
  };

  async create(username, password, name, email): Promise<void> {
    await this.navigateTo('create');

    // fill in fields
    await element(by.id('username')).sendKeys(username);
    await element(by.id('password')).sendKeys(password);
    await element(by.id('confirmpassword')).sendKeys(password);
    await element(by.id('name')).sendKeys(name);
    await element(by.id('email')).sendKeys(email);

    // Click the create button and wait for user to be made
    const createButton = element(by.cssContainingText('button', 'Create User'));
    await createButton.click();
    await this.waitPresence(this.getTableCell(username));
  }

  async edit(username, password, name, email): Promise<void> {
    await this.navigateTo();

    await this.getTableCell(username).click(); // select user from table
    await element(by.cssContainingText('button', 'Edit')).click(); // click button to move to edit page

    // fill in fields with new values
    await element(by.id('password')).clear();
    await element(by.id('password')).sendKeys(password);
    await element(by.id('confirmpassword')).clear();
    await element(by.id('confirmpassword')).sendKeys(password);
    await element(by.id('name')).clear();
    await element(by.id('name')).sendKeys(name);
    await element(by.id('email')).clear();
    await element(by.id('email')).sendKeys(email);

    // Click the edit button and check new values are present in table
    const editButton = element(by.cssContainingText('button', 'Edit User'));
    await editButton.click();
    await this.waitPresence(this.getTableCell(email));
    await this.waitPresence(this.getTableCell(name));
  }

  async userDelete(username): Promise<void> {
    await this.navigateTo();

    await this.getTableCell(username).click(); // select user from table
    await $('.table-actions button.dropdown-toggle').click(); // click toggle menu
    await $('li.delete a').click(); // click delete

    await this.waitVisibility($('.custom-control-label'));
    await $('.custom-control-label').click(); // click confirmation checkbox
    await element(by.cssContainingText('button', 'Delete User')).click();
    await this.waitStaleness(this.getFirstTableCellWithText(username));
  }
}
