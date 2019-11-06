import { $, by, element } from 'protractor';
import { protractor } from 'protractor/built/ptor';
import { PageHelper } from '../page-helper.po';

const pages = {
  index: '/#/rgw/user',
  create: '/#/rgw/user/create'
};

export class UsersPageHelper extends PageHelper {
  pages = pages;

  @PageHelper.restrictTo(pages.create)
  async create(username, fullname, email, maxbuckets) {
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
    await this.waitPresence(this.getFirstTableCellWithText(username));
  }

  @PageHelper.restrictTo(pages.index)
  async edit(name, new_fullname, new_email, new_maxbuckets) {
    await this.waitClickableAndClick(this.getFirstTableCellWithText(name)); // wait for table to load and click
    await element(by.cssContainingText('button', 'Edit')).click(); // click button to move to edit page

    await this.waitTextToBePresent(this.getBreadcrumb(), 'Edit');

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
    // Click the user and check its details table for updated content
    await this.waitClickableAndClick(this.getFirstTableCellWithText(name));
    await expect($('.active.tab-pane').getText()).toMatch(new_fullname); // check full name was changed
    await expect($('.active.tab-pane').getText()).toMatch(new_email); // check email was changed
    await expect($('.active.tab-pane').getText()).toMatch(new_maxbuckets); // check max buckets was changed
  }

  async invalidCreate() {
    const uname = '000invalid_create_user';
    // creating this user in order to check that you can't give two users the same name
    await this.navigateTo('create');
    await this.create(uname, 'xxx', 'xxx@xxx', '1');

    await this.navigateTo('create');

    const username_field = element(by.id('uid'));

    // No username had been entered. Field should be invalid
    await expect(username_field.getAttribute('class')).toContain('ng-invalid');

    // Try to give user already taken name. Should make field invalid.
    await username_field.clear();
    await username_field.sendKeys(uname);
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

    await this.navigateTo();
    await this.delete(uname);
  }

  async invalidEdit() {
    const uname = '000invalid_edit_user';
    // creating this user to edit for the test
    await this.navigateTo('create');
    await this.create(uname, 'xxx', 'xxx@xxx', '1');

    await this.navigateTo();

    // wait for table to load and click on the bucket you want to edit in the table
    await this.waitClickableAndClick(this.getFirstTableCellWithText(uname));
    await element(by.cssContainingText('button', 'Edit')).click(); // click button to move to edit page

    await this.waitTextToBePresent(this.getBreadcrumb(), 'Edit');

    // put invalid email to make field invalid
    await element(by.id('email')).click();
    await element(by.id('email')).clear();
    await element(by.id('email')).sendKeys('a');
    await this.waitFn(
      async () => !(await element(by.id('email')).getAttribute('class')).includes('ng-pending')
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

    await this.navigateTo();
    await this.delete(uname);
  }
}
