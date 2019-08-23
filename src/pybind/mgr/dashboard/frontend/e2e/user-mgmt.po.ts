import { $, browser, by, element } from 'protractor';
import { Helper } from './helper.po';
import { PageHelper } from './page-helper.po';

export class UserMgmtPageHelper extends PageHelper {
  pages = {
    index: '/#/user-management',
    users: '/#/user-management/users',
    userCreate: '/#/user-management/users/create',
    roles: '/#/user-management/roles',
    roleCreate: '/#/user-management/roles/create'
  };

  userCreate(username, password, name, email) {
    this.navigateTo('userCreate');

    // fill in fields
    element(by.id('username')).sendKeys(username);
    element(by.id('password')).sendKeys(password);
    element(by.id('confirmpassword')).sendKeys(password);
    element(by.id('name')).sendKeys(name);
    element(by.id('email')).sendKeys(email);

    // Click the create button and wait for user to be made
    const createButton = element(by.cssContainingText('button', 'Create User'));
    this.moveClick(createButton).then(() => {
      browser.wait(Helper.EC.presenceOf(this.getTableCell(username)), Helper.TIMEOUT);
    });
  }

  userEdit(username, password, name, email) {
    this.navigateTo('users');

    this.getTableCell(username).click(); // select user from table
    element(by.cssContainingText('button', 'Edit')).click(); // click button to move to edit page

    // fill in fields with new values
    element(by.id('password')).clear();
    element(by.id('password')).sendKeys(password);
    element(by.id('confirmpassword')).clear();
    element(by.id('confirmpassword')).sendKeys(password);
    element(by.id('name')).clear();
    element(by.id('name')).sendKeys(name);
    element(by.id('email')).clear();
    element(by.id('email')).sendKeys(email);

    // Click the edit button and check new values are present in table
    const editButton = element(by.cssContainingText('button', 'Edit User'));
    this.moveClick(editButton).then(() => {
      browser.wait(Helper.EC.presenceOf(this.getTableCell(email)), Helper.TIMEOUT);
      browser.wait(Helper.EC.presenceOf(this.getTableCell(name)), Helper.TIMEOUT);
    });
  }

  userDelete(username) {
    this.navigateTo('users');

    this.getTableCell(username).click(); // select user from table
    $('.table-actions button.dropdown-toggle').click(); // click toggle menu
    $('li.delete a').click(); // click delete

    browser.wait(Helper.EC.visibilityOf($('.custom-control-label')), Helper.TIMEOUT);
    $('.custom-control-label').click(); // click confirmation checkbox
    element(by.cssContainingText('button', 'Delete User')) // click delete user button
      .click()
      .then(() => {
        browser.wait(
          Helper.EC.stalenessOf(this.getFirstTableCellWithText(username)),
          Helper.TIMEOUT
        );
      });
  }

  roleCreate(name, description) {
    this.navigateTo('roleCreate');

    // fill in fields
    element(by.id('name')).sendKeys(name);
    element(by.id('description')).sendKeys(description);

    // Click the create button and wait for user to be made
    const createButton = element(by.cssContainingText('button', 'Create Role'));
    this.moveClick(createButton).then(() => {
      browser.wait(Helper.EC.presenceOf(this.getTableCell(name)), Helper.TIMEOUT);
    });
  }

  roleEdit(name, description) {
    this.navigateTo('roles');

    this.getTableCell(name).click(); // select role from table
    element(by.cssContainingText('button', 'Edit')).click(); // click button to move to edit page

    // fill in fields with new values
    element(by.id('description')).clear();
    element(by.id('description')).sendKeys(description);

    // Click the edit button and check new values are present in table
    const editButton = element(by.cssContainingText('button', 'Edit Role'));
    this.moveClick(editButton).then(() => {
      browser.wait(Helper.EC.presenceOf(this.getTableCell(name)), Helper.TIMEOUT);
      browser.wait(Helper.EC.presenceOf(this.getTableCell(description)), Helper.TIMEOUT);
    });
  }

  roleDelete(name) {
    this.navigateTo('roles');

    this.getTableCell(name).click(); // select role from table
    $('.table-actions button.dropdown-toggle').click(); // click toggle menu
    $('li.delete a').click(); // click delete

    browser.wait(Helper.EC.visibilityOf($('.custom-control-label')), Helper.TIMEOUT);
    $('.custom-control-label').click(); // click confirmation checkbox
    element(by.cssContainingText('button', 'Delete Role')) // click delete user button
      .click()
      .then(() => {
        browser.wait(Helper.EC.stalenessOf(this.getFirstTableCellWithText(name)), Helper.TIMEOUT);
      });
  }
}
