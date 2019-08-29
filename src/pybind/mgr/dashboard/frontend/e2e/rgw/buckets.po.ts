import { $, by, element } from 'protractor';
import { PageHelper } from '../page-helper.po';

const pages = {
  index: '/#/rgw/bucket',
  create: '/#/rgw/bucket/create'
};

export class BucketsPageHelper extends PageHelper {
  pages = pages;

  /**
   * TODO add check to verify the existance of the bucket!
   * TODO let it print a meaningful error message (for devs) if it does not exist!
   */
  @PageHelper.restrictTo(pages.create)
  async create(name: string, owner: string, placementTarget: string) {
    // Enter in bucket name
    await element(by.id('bid')).sendKeys(name);

    // Select bucket owner
    await element(by.id('owner')).click();
    await element(by.cssContainingText('select[name=owner] option', owner)).click();
    await expect(element(by.id('owner')).getAttribute('class')).toContain('ng-valid');

    // Select bucket placement target:
    await element(by.id('owner')).click();
    await element(
      by.cssContainingText('select[name=placement-target] option', placementTarget)
    ).click();
    await expect(element(by.id('placement-target')).getAttribute('class')).toContain('ng-valid');

    // Click the create button and wait for bucket to be made
    const createButton = element(by.cssContainingText('button', 'Create Bucket'));
    await createButton.click();

    return this.waitPresence(this.getTableCell(name), 'Timed out waiting for bucket creation');
  }

  @PageHelper.restrictTo(pages.index)
  async edit(name: string, new_owner: string) {
    await this.getTableCell(name).click(); // click on the bucket you want to edit in the table
    await element(by.cssContainingText('button', 'Edit')).click(); // click button to move to edit page
    await expect(this.getBreadcrumbText()).toEqual('Edit');
    await expect(element(by.css('input[name=placement-target]')).getAttribute('value')).toBe(
      'default-placement'
    );
    await element(by.id('owner')).click(); // click owner dropdown menu
    await element(by.cssContainingText('select[name=owner] option', new_owner)).click(); // select the new user
    await element(by.cssContainingText('button', 'Edit Bucket')).click();

    // wait to be back on buckets page with table visible
    await this.waitClickable(
      this.getTableCell(name),
      'Could not return to buckets page and load table after editing bucket'
    );

    // click on edited bucket and check its details table for edited owner field
    const promise = await this.getTableCell(name).click();
    const element_details_table = element
      .all(by.css('.table.table-striped.table-bordered'))
      .first();
    await expect(element_details_table.getText()).toMatch(new_owner);
    return promise;
  }

  @PageHelper.restrictTo(pages.index)
  async delete(name) {
    // wait for table to load
    await this.waitClickable(this.getTableCell(name));

    await this.getTableCell(name).click(); // click on the bucket you want to delete in the table
    await $('.table-actions button.dropdown-toggle').click(); // click toggle menu
    await $('li.delete a').click(); // click delete
    // wait for pop-up to be visible (checks for title of pop-up)
    await this.waitVisibility($('.modal-title.float-left'));
    await this.waitVisibility($('.custom-control-label'));
    await $('.custom-control-label').click();
    await element(by.cssContainingText('button', 'Delete bucket')).click();
    await this.navigateTo();
    return this.waitStaleness(this.getTableCell(name));
  }

  async testInvalidCreate() {
    await this.navigateTo('create');
    const nameInputField = element(by.id('bid')); // Grabs name box field
    const ownerDropDown = element(by.id('owner')); // Grab owner field

    // Gives an invalid name (too short), then waits for dashboard to determine validity
    await nameInputField.sendKeys('rq');

    await ownerDropDown.click(); // To trigger a validation

    await this.waitFn(async () => {
      // Waiting for website to decide if name is valid or not
      const klass = await nameInputField.getAttribute('class');
      return !klass.includes('ng-pending');
    }, 'Timed out waiting for dashboard to decide bucket name validity');

    // Check that name input field was marked invalid in the css
    await expect(nameInputField.getAttribute('class')).toContain('ng-invalid');

    // Check that error message was printed under name input field
    await expect(element(by.css('#bid + .invalid-feedback')).getText()).toMatch(
      'The value is not valid.'
    );

    // Test invalid owner input
    await ownerDropDown.click(); // Clicks the Owner drop down on the Create Bucket page
    // select some valid option. The owner drop down error message will not appear unless a valid user was selected at
    // one point before the invalid placeholder user is selected.
    await element(by.cssContainingText('select[name=owner] option', 'dev')).click();

    await ownerDropDown.click();
    // select the first option, which is invalid because it is a placeholder
    await element(by.cssContainingText('select[name=owner] option', 'Select a user')).click();

    await nameInputField.click();

    // Check that owner drop down field was marked invalid in the css
    await expect(element(by.id('owner')).getAttribute('class')).toContain('ng-invalid');

    // Check that error message was printed under owner drop down field
    await expect(element(by.css('#owner + .invalid-feedback')).getText()).toMatch(
      'This field is required.'
    );

    // Check invalid placement target input
    await ownerDropDown.click();
    await element(by.cssContainingText('select[name=owner] option', 'dev')).click();
    // The drop down error message will not appear unless a valid option is previsously selected.
    await element(
      by.cssContainingText('select[name=placement-target] option', 'default-placement')
    ).click();
    await element(
      by.cssContainingText('select[name=placement-target] option', 'Select a placement target')
    ).click();
    await nameInputField.click(); // Trigger validation
    await expect(element(by.id('placement-target')).getAttribute('class')).toContain('ng-invalid');
    await expect(element(by.css('#placement-target + .invalid-feedback')).getText()).toMatch(
      'This field is required.'
    );

    // Clicks the Create Bucket button but the page doesn't move. Done by testing
    // for the breadcrumb
    await element(by.cssContainingText('button', 'Create Bucket')).click(); // Clicks Create Bucket button
    await expect(this.getBreadcrumbText()).toEqual('Create');
    // content in fields seems to subsist through tests if not cleared, so it is cleared
    await nameInputField.clear();
    return element(by.cssContainingText('button', 'Cancel')).click();
  }

  async testInvalidEdit(name) {
    await this.navigateTo();

    await this.waitClickable(
      this.getTableCell(name),
      'Failed waiting for bucket to be present in table'
    ); // wait for table to load
    await this.getTableCell(name).click(); // click on the bucket you want to edit in the table
    await element(by.cssContainingText('button', 'Edit')).click(); // click button to move to edit page

    await expect(this.getBreadcrumbText()).toEqual('Edit');

    // Chooses 'Select a user' rather than a valid owner on Edit Bucket page
    // and checks if it's an invalid input
    const ownerDropDown = element(by.id('owner'));
    await this.waitClickable(ownerDropDown);
    await ownerDropDown.click(); // Clicks the Owner drop down on the Create Bucket page

    // select the first option, which is invalid because it is a placeholder
    await element(by.cssContainingText('select[name=owner] option', 'Select a user')).click();

    // Changes when updated to bootstrap 4 -> Error message takes a long time to appear unless another field
    // is clicked on. For that reason, I'm having the test click on the edit button before checking for errors
    await element(by.cssContainingText('button', 'Edit Bucket')).click();

    // Check that owner drop down field was marked invalid in the css
    await expect(element(by.id('owner')).getAttribute('class')).toContain('ng-invalid');

    // Check that error message was printed under owner drop down field
    await expect(element(by.css('#owner + .invalid-feedback')).getText()).toMatch(
      'This field is required.'
    );

    await expect(this.getBreadcrumbText()).toEqual('Edit');
  }
}
