import { $, browser, by, element } from 'protractor';
import { Helper } from '../helper.po';
import { PageHelper } from '../page-helper.po';

export class BucketsPageHelper extends PageHelper {
  pages = {
    index: '/#/rgw/bucket',
    create: '/#/rgw/bucket/create'
  };

  create(name, owner, placementTarget) {
    this.navigateTo('create');

    // Enter in bucket name
    element(by.id('bid')).sendKeys(name);

    // Select bucket owner
    element(by.id('owner')).click();
    element(by.cssContainingText('select[name=owner] option', owner)).click();
    expect(element(by.id('owner')).getAttribute('class')).toContain('ng-valid');

    // Select bucket placement target:
    element(by.id('owner')).click();
    element(by.cssContainingText('select[name=placement-target] option', placementTarget)).click();
    expect(element(by.id('placement-target')).getAttribute('class')).toContain('ng-valid');

    // Click the create button and wait for bucket to be made
    const createButton = element(by.cssContainingText('button', 'Create Bucket'));
    createButton.click().then(() => {
      browser.wait(
        Helper.EC.presenceOf(this.getTableCell(name)),
        Helper.TIMEOUT,
        'Timed out waiting for bucket creation'
      );
    });
  }

  edit(name, new_owner) {
    this.navigateTo();

    browser.wait(Helper.EC.elementToBeClickable(this.getTableCell(name)), 10000); // wait for table to load
    this.getTableCell(name).click(); // click on the bucket you want to edit in the table
    element(by.cssContainingText('button', 'Edit')).click(); // click button to move to edit page

    expect(this.getBreadcrumbText()).toEqual('Edit');

    expect(element(by.css('input[name=placement-target]')).getAttribute('value')).toBe(
      'default-placement'
    );

    const ownerDropDown = element(by.id('owner'));
    ownerDropDown.click(); // click owner dropdown menu

    element(by.cssContainingText('select[name=owner] option', new_owner)).click(); // select the new user
    const editbutton = element(by.cssContainingText('button', 'Edit Bucket'));
    editbutton.click().then(() => {
      // wait to be back on buckets page with table visible
      browser.wait(
        Helper.EC.elementToBeClickable(this.getTableCell(name)),
        10000,
        'Could not return to buckets page and load table after editing bucket'
      );

      // click on edited bucket and check its details table for edited owner field
      this.getTableCell(name).click();
      const element_details_table = element
        .all(by.css('.table.table-striped.table-bordered'))
        .first();
      expect(element_details_table.getText()).toMatch(new_owner);
    });
  }

  delete(name) {
    this.navigateTo();

    // wait for table to load
    browser.wait(Helper.EC.elementToBeClickable(this.getTableCell(name)), 10000);

    this.getTableCell(name).click(); // click on the bucket you want to delete in the table
    $('.table-actions button.dropdown-toggle').click(); // click toggle menu
    $('li.delete a').click(); // click delete
    // wait for pop-up to be visible (checks for title of pop-up)
    browser.wait(Helper.EC.visibilityOf($('.modal-title.float-left')), 10000).then(() => {
      browser.wait(Helper.EC.visibilityOf($('.custom-control-label')), 5000);
      $('.custom-control-label').click();
      element(by.cssContainingText('button', 'Delete bucket'))
        .click()
        .then(() => {
          this.navigateTo();
          browser.wait(Helper.EC.not(Helper.EC.presenceOf(this.getTableCell(name))), 10000);
        });
    });
  }

  invalidCreate() {
    this.navigateTo('create');
    expect(this.getBreadcrumbText()).toEqual('Create');

    const nameInputField = element(by.id('bid')); // Grabs name box field
    const ownerDropDown = element(by.id('owner')); // Grab owner field

    // Gives an invalid name (too short), then waits for dashboard to determine validity
    nameInputField.sendKeys('rq');

    this.moveClick(ownerDropDown); // To trigger a validation

    browser.wait(
      function() {
        // Waiting for website to decide if name is valid or not
        return nameInputField.getAttribute('class').then(function(classValue) {
          return classValue.indexOf('ng-pending') === -1;
        });
      },
      5000,
      'Timed out waiting for dashboard to decide bucket name validity'
    );

    // Check that name input field was marked invalid in the css
    expect(nameInputField.getAttribute('class')).toContain('ng-invalid');

    // Check that error message was printed under name input field
    expect(element(by.css('#bid + .invalid-feedback')).getText()).toMatch(
      'The value is not valid.'
    );

    // Test invalid owner input
    this.moveClick(ownerDropDown); // Clicks the Owner drop down on the Create Bucket page
    // select some valid option. The owner drop down error message will not appear unless a valid user was selected at
    // one point before the invalid placeholder user is selected.
    element(by.cssContainingText('select[name=owner] option', 'dev')).click();

    this.moveClick(ownerDropDown); // Clicks the Owner drop down on the Create Bucket page
    // select the first option, which is invalid because it is a placeholder
    element(by.cssContainingText('select[name=owner] option', 'Select a user')).click();

    this.moveClick(nameInputField); // To trigger a validation

    // Check that owner drop down field was marked invalid in the css
    expect(element(by.id('owner')).getAttribute('class')).toContain('ng-invalid');

    // Check that error message was printed under owner drop down field
    expect(element(by.css('#owner + .invalid-feedback')).getText()).toMatch(
      'This field is required.'
    );

    // Check invalid placement target input
    this.moveClick(ownerDropDown);
    element(by.cssContainingText('select[name=owner] option', 'dev')).click();
    // The drop down error message will not appear unless a valid option is previsously selected.
    element(
      by.cssContainingText('select[name=placement-target] option', 'default-placement')
    ).click();
    element(
      by.cssContainingText('select[name=placement-target] option', 'Select a placement target')
    ).click();
    this.moveClick(nameInputField); // To trigger a validation
    expect(element(by.id('placement-target')).getAttribute('class')).toContain('ng-invalid');
    expect(element(by.css('#placement-target + .invalid-feedback')).getText()).toMatch(
      'This field is required.'
    );

    // Clicks the Create Bucket button but the page doesn't move. Done by testing
    // for the breadcrumb
    this.moveClick(element(by.cssContainingText('button', 'Create Bucket'))); // Clicks Create Bucket button
    expect(this.getBreadcrumbText()).toEqual('Create');
    // content in fields seems to subsist through tests if not cleared, so it is cleared
    nameInputField.clear().then(() => {
      element(by.cssContainingText('button', 'Cancel')).click();
    });
  }

  invalidEdit(name) {
    this.navigateTo();

    browser.wait(
      Helper.EC.elementToBeClickable(this.getTableCell(name)),
      10000,
      'Failed waiting for bucket to be present in table'
    ); // wait for table to load
    this.getTableCell(name).click(); // click on the bucket you want to edit in the table
    element(by.cssContainingText('button', 'Edit')).click(); // click button to move to edit page

    expect(this.getBreadcrumbText()).toEqual('Edit');

    // Chooses 'Select a user' rather than a valid owner on Edit Bucket page
    // and checks if it's an invalid input
    const ownerDropDown = element(by.id('owner'));
    browser.wait(Helper.EC.elementToBeClickable(ownerDropDown), 5000);

    this.moveClick(ownerDropDown); // Clicks the Owner drop down on the Create Bucket page
    // select the first option, which is invalid because it is a placeholder
    element(by.cssContainingText('select[name=owner] option', 'Select a user')).click();

    // Changes when updated to bootstrap 4 -> Error message takes a long time to appear unless another field
    // is clicked on. For that reason, I'm having the test click on the nedit button before checking for errors
    element(by.cssContainingText('button', 'Edit Bucket')).click();

    // Check that owner drop down field was marked invalid in the css
    expect(element(by.id('owner')).getAttribute('class')).toContain('ng-invalid');

    // Check that error message was printed under owner drop down field
    expect(element(by.css('#owner + .invalid-feedback')).getText()).toMatch(
      'This field is required.'
    );

    // Clicks the Edit Bucket button but the page doesn't move. Done by testing for
    // breadcrumb
    element(by.cssContainingText('button', 'Edit Bucket')).click(); // Gets the Edit button and clicks it
    expect(this.getBreadcrumbText()).toEqual('Edit');
  }
}
