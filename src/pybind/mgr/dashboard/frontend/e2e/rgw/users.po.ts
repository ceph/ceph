import { $, browser, by, element } from 'protractor';
import { protractor } from 'protractor/built/ptor';
import { Helper } from '../helper.po';
import { PageHelper } from '../page-helper.po';

export class UsersPageHelper extends PageHelper {
  pages = {
    index: '/#/rgw/user',
    create: '/#/rgw/user/create'
  };

  async create(username, fullname, email, maxbuckets) {
    await this.navigateTo('create');

    // Enter in  username
    await element(by.id('uid')).sendKeys(username);

    // Enter in full name
    await element(by.id('display_name')).click();
    await element(by.id('display_name')).sendKeys(fullname);

    // Enter in email
    await element(by.id('email')).click();
    await element(by.id('email')).sendKeys(email);

    // Enter max buckets
    await element(by.id('max_buckets')).click();
    await element(by.id('max_buckets')).clear();
    await element(by.id('max_buckets')).sendKeys(maxbuckets);

    // Click the create button and wait for user to be made
    await element(by.cssContainingText('button', 'Create User')).click();
    await browser.wait(Helper.EC.presenceOf(this.getTableCell(username)), 10000);
  }

  async edit(name, new_fullname, new_email, new_maxbuckets) {
    await this.navigateTo();

    await browser.wait(Helper.EC.elementToBeClickable(this.getTableCell(name)), 10000); // wait for table to load
    await this.getTableCell(name).click(); // click on the bucket you want to edit in the table
    await element(by.cssContainingText('button', 'Edit')).click(); // click button to move to edit page

    await expect(this.getBreadcrumbText()).toEqual('Edit');

    // Change the full name field
    await element(by.id('display_name')).click();
    await element(by.id('display_name')).clear();
    await element(by.id('display_name')).sendKeys(new_fullname);

    // Change the email field
    await element(by.id('email')).click();
    await element(by.id('email')).clear();
    await element(by.id('email')).sendKeys(new_email);

    // Change the max buckets field
    await element(by.id('max_buckets')).click();
    await element(by.id('max_buckets')).clear();
    await element(by.id('max_buckets')).sendKeys(new_maxbuckets);

    const editbutton = element(by.cssContainingText('button', 'Edit User'));
    await editbutton.click();
    await browser.wait(Helper.EC.elementToBeClickable(this.getTableCell(name)), 10000);
    // Click the user and check its details table for updated content
    await this.getTableCell(name).click();
    await expect($('.active.tab-pane').getText()).toMatch(new_fullname); // check full name was changed
    await expect($('.active.tab-pane').getText()).toMatch(new_email); // check email was changed
    await expect($('.active.tab-pane').getText()).toMatch(new_maxbuckets); // check max buckets was changed
  }

  async delete(name) {
    await this.navigateTo();

    // wait for table to load
    const my_user = this.getFirstTableCellWithText(name);
    await browser.wait(Helper.EC.elementToBeClickable(my_user), 10000);

    await my_user.click(); // click on the user you want to delete in the table
    await $('.table-actions button.dropdown-toggle').click(); // click toggle menu
    await $('li.delete a').click(); // click delete

    // wait for pop-up to be visible (checks for title of pop-up)
    await browser.wait(Helper.EC.visibilityOf($('.modal-title.float-left')), 10000);
    await browser.wait(Helper.EC.visibilityOf($('.custom-control-label')), 5000);
    await $('.custom-control-label').click(); // click confirmation checkbox
    await element(by.cssContainingText('button', 'Delete user')).click();
    await browser.wait(
      Helper.EC.not(Helper.EC.presenceOf(this.getFirstTableCellWithText(name))),
      10000
    );
  }

  async invalidCreate() {
    const uname = '000invalid_create_user';
    // creating this user in order to check that you can't give two users the same name
    await this.create(uname, 'xxx', 'xxx@xxx', '1');

    await this.navigateTo('create');

    const username_field = element(by.id('uid'));

    // No username had been entered. Field should be invalid
    await expect(username_field.getAttribute('class')).toContain('ng-invalid');

    // Try to give user already taken name. Should make field invalid.
    await username_field.clear();
    await username_field.sendKeys(uname);
    await browser.wait(
      async () => !(await username_field.getAttribute('class')).includes('ng-pending'),
      6000
    );
    await expect(username_field.getAttribute('class')).toContain('ng-invalid');
    await element(by.id('display_name')).click(); // trigger validation check
    await expect(element(by.css('#uid + .invalid-feedback')).getText()).toMatch(
      'The chosen user ID is already in use.'
    );

    // check that username field is marked invalid if username has been cleared off
    for (let i = 0; i < uname.length; i++) {
      await username_field.sendKeys(protractor.Key.BACK_SPACE);
    }
    await expect(username_field.getAttribute('class')).toContain('ng-invalid');
    await element(by.id('display_name')).click(); // trigger validation check
    await expect(element(by.css('#uid + .invalid-feedback')).getText()).toMatch(
      'This field is required.'
    );

    // No display name has been given so field should be invalid
    await expect(element(by.id('display_name')).getAttribute('class')).toContain('ng-invalid');

    // display name field should also be marked invalid if given input then emptied
    await element(by.id('display_name')).click();
    await element(by.id('display_name')).sendKeys('a');
    await element(by.id('display_name')).sendKeys(protractor.Key.BACK_SPACE);
    await expect(element(by.id('display_name')).getAttribute('class')).toContain('ng-invalid');
    await username_field.click(); // trigger validation check
    await expect(element(by.css('#display_name + .invalid-feedback')).getText()).toMatch(
      'This field is required.'
    );

    // put invalid email to make field invalid
    await element(by.id('email')).click();
    await element(by.id('email')).sendKeys('a');
    await expect(element(by.id('email')).getAttribute('class')).toContain('ng-invalid');
    await username_field.click(); // trigger validation check
    await expect(element(by.css('#email + .invalid-feedback')).getText()).toMatch(
      'This is not a valid email address.'
    );

    // put negative max buckets to make field invalid
    await element(by.id('max_buckets')).click();
    await element(by.id('max_buckets')).clear();
    await element(by.id('max_buckets')).sendKeys('-5');
    await expect(element(by.id('max_buckets')).getAttribute('class')).toContain('ng-invalid');
    await username_field.click(); // trigger validation check
    await expect(element(by.css('#max_buckets + .invalid-feedback')).getText()).toMatch(
      'The entered value must be >= 0.'
    );

    await this.delete(uname);
  }

  async invalidEdit() {
    const uname = '000invalid_edit_user';
    // creating this user to edit for the test
    await this.create(uname, 'xxx', 'xxx@xxx', '1');

    await this.navigateTo();

    await browser.wait(Helper.EC.elementToBeClickable(this.getTableCell(uname)), 10000); // wait for table to load
    await this.getTableCell(uname).click(); // click on the bucket you want to edit in the table
    await element(by.cssContainingText('button', 'Edit')).click(); // click button to move to edit page

    await expect(this.getBreadcrumbText()).toEqual('Edit');

    // put invalid email to make field invalid
    await element(by.id('email')).click();
    await element(by.id('email')).clear();
    await element(by.id('email')).sendKeys('a');
    await browser.wait(
      async () => !(await element(by.id('email')).getAttribute('class')).includes('ng-pending'),
      6000
    );
    await expect(element(by.id('email')).getAttribute('class')).toContain('ng-invalid');
    await element(by.id('display_name')).click(); // trigger validation check
    await expect(element(by.css('#email + .invalid-feedback')).getText()).toMatch(
      'This is not a valid email address.'
    );

    // empty the display name field making it invalid
    await element(by.id('display_name')).click();
    for (let i = 0; i < 3; i++) {
      await element(by.id('display_name')).sendKeys(protractor.Key.BACK_SPACE);
    }
    await expect(element(by.id('display_name')).getAttribute('class')).toContain('ng-invalid');
    await element(by.id('email')).click(); // trigger validation check
    await expect(element(by.css('#display_name + .invalid-feedback')).getText()).toMatch(
      'This field is required.'
    );

    // put negative max buckets to make field invalid
    await element(by.id('max_buckets')).click();
    await element(by.id('max_buckets')).clear();
    await element(by.id('max_buckets')).sendKeys('-5');
    await expect(element(by.id('max_buckets')).getAttribute('class')).toContain('ng-invalid');
    await element(by.id('email')).click(); // trigger validation check
    await expect(element(by.css('#max_buckets + .invalid-feedback')).getText()).toMatch(
      'The entered value must be >= 0.'
    );

    await this.delete(uname);
  }
}
