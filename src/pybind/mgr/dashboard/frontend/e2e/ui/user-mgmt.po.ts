import { $, by, element } from 'protractor';
import { PageHelper } from '../page-helper.po';

export class UserMgmtPageHelper extends PageHelper {
  pages = {
    index: '/#/user-management',
    users: '/#/user-management/users',
    userCreate: '/#/user-management/users/create',
    roles: '/#/user-management/roles',
    roleCreate: '/#/user-management/roles/create'
  };

  async userCreate(username, password, name, email): Promise<void> {
    await this.navigateTo('userCreate');

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

  async userEdit(username, password, name, email): Promise<void> {
    await this.navigateTo('users');

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
    await this.navigateTo('users');

    await this.getTableCell(username).click(); // select user from table
    await $('.table-actions button.dropdown-toggle').click(); // click toggle menu
    await $('li.delete a').click(); // click delete

    await this.waitVisibility($('.custom-control-label'));
    await $('.custom-control-label').click(); // click confirmation checkbox
    await element(by.cssContainingText('button', 'Delete User')).click();
    await this.waitStaleness(this.getFirstTableCellWithText(username));
  }

  async roleCreate(name, description): Promise<void> {
    await this.navigateTo('roleCreate');

    // fill in fields
    await element(by.id('name')).sendKeys(name);
    await element(by.id('description')).sendKeys(description);

    // Click the create button and wait for user to be made
    const createButton = element(by.cssContainingText('button', 'Create Role'));
    await createButton.click();
    await this.waitPresence(this.getTableCell(name));
  }

  async roleEdit(name, description): Promise<void> {
    await this.navigateTo('roles');

    await this.getTableCell(name).click(); // select role from table
    await element(by.cssContainingText('button', 'Edit')).click(); // click button to move to edit page

    // fill in fields with new values
    await element(by.id('description')).clear();
    await element(by.id('description')).sendKeys(description);

    // Click the edit button and check new values are present in table
    const editButton = element(by.cssContainingText('button', 'Edit Role'));
    await editButton.click();

    await this.waitPresence(this.getTableCell(name));
    await this.waitPresence(this.getTableCell(description));
  }

  async roleDelete(name) {
    await this.navigateTo('roles');

    await this.getTableCell(name).click(); // select role from table
    await $('.table-actions button.dropdown-toggle').click(); // click toggle menu
    await $('li.delete a').click(); // click delete

    await this.waitVisibility($('.custom-control-label'));
    await $('.custom-control-label').click(); // click confirmation checkbox
    await element(by.cssContainingText('button', 'Delete Role')).click();
    await this.waitStaleness(this.getFirstTableCellWithText(name));
  }
}
