import { $, browser, by, element } from 'protractor';
import { Helper } from '../helper.po';
import { PageHelper } from '../page-helper.po';

export class BucketsPageHelper extends PageHelper {
  pages = {
    index: '/#/rgw/bucket',
    create: '/#/rgw/bucket/create'
  };

  create(name, owner) {
    this.navigateTo('create');
    expect(PageHelper.getTitleText()).toBe('Create Bucket');

    // Need the string '[value="<owner>"]' to find the owner in dropdown menu
    const getOwnerStr = `[value="${owner}"]`;

    // Enter in bucket name
    element(by.id('bid')).sendKeys(name);

    // Select bucket owner
    PageHelper.moveClick(element(by.id('owner')));
    $(getOwnerStr).click();
    expect(element(by.id('owner')).getAttribute('class')).toContain('ng-valid');

    // Click the create button and wait for bucket to be made
    const createButton = element(by.cssContainingText('button', 'Create Bucket'));
    createButton.click().then(() => {
      browser.wait(
        Helper.EC.presenceOf(PageHelper.getTableCell(name)),
        Helper.TIMEOUT,
        'Timed out waiting for bucket creation'
      );
    });
  }

  edit(name, new_owner) {
    this.navigateTo();

    browser.wait(Helper.EC.elementToBeClickable(PageHelper.getTableCell(name)), 10000); // wait for table to load
    PageHelper.getTableCell(name).click(); // click on the bucket you want to edit in the table
    element(by.cssContainingText('button', 'Edit')).click(); // click button to move to edit page

    expect(PageHelper.getBreadcrumbText()).toEqual('Edit');

    const ownerDropDown = element(by.id('owner'));
    PageHelper.moveClick(ownerDropDown); // click owner dropdown menu

    // Need the string '[value="<owner>"]' to find the new owner in dropdown menu
    const getOwnerStr = `[value="${new_owner}"]`;

    $(getOwnerStr).click(); // select the new user
    const editbutton = element(by.cssContainingText('button', 'Edit Bucket'));
    editbutton.click().then(() => {
      // wait to be back on buckets page with table visible
      browser.wait(
        Helper.EC.elementToBeClickable(PageHelper.getTableCell(name)),
        10000,
        'Could not return to buckets page and load table after editing bucket'
      );

      // click on edited bucket and check its details table for edited owner field
      PageHelper.getTableCell(name).click();
      const element_details_table = element
        .all(by.css('.table.table-striped.table-bordered'))
        .first();
      expect(element_details_table.getText()).toMatch(new_owner);
    });
  }

  delete(name) {
    this.navigateTo();

    // wait for table to load
    browser.wait(Helper.EC.elementToBeClickable(PageHelper.getTableCell(name)), 10000);

    PageHelper.getTableCell(name).click(); // click on the bucket you want to delete in the table
    $('.table-actions button.dropdown-toggle').click(); // click toggle menu
    $('li.delete a').click(); // click delete
    // wait for pop-up to be visible (checks for title of pop-up)
    browser
      .wait(Helper.EC.visibilityOf(element(by.css('.modal-title.pull-left'))), 5000)
      .then(() => {
        $('#confirmation').click(); // click confirmation checkbox
        element(by.cssContainingText('button', 'Delete bucket')).click(); // click delete button
        // wait for bucket to be deleted
        browser.wait(
          Helper.EC.not(Helper.EC.presenceOf(PageHelper.getTableCell(name))),
          10000,
          'Timed out waiting for bucket deletion'
        );
      });
  }

  invalidCreate() {
    this.navigateTo('create');
    expect(PageHelper.getTitleText()).toBe('Create Bucket');

    const nameInputField = element(by.id('bid')); // Grabs name box field

    // Gives an invalid name (too short), then waits for dashboard to determine validity
    nameInputField.sendKeys('rq');
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
    expect(element(by.cssContainingText('.form-group', 'Name')).getText()).toMatch(
      'The value is not valid.'
    );

    // Check that dashboard detects invalid owner
    const ownerDropDown = element(by.id('owner'));
    browser.wait(Helper.EC.elementToBeClickable(ownerDropDown), 5000);

    PageHelper.moveClick(ownerDropDown); // Clicks the Owner drop down on the Create Bucket page
    // select some valid option. The owner drop down error message will not appear unless a valid user was selected at
    // one point before the invalid placeholder user is selected.
    ownerDropDown
      .all(by.css('option'))
      .last()
      .click();

    PageHelper.moveClick(ownerDropDown); // Clicks the Owner drop down on the Create Bucket page
    ownerDropDown
      .all(by.css('option'))
      .first()
      .click(); // select the first option, which is invalid because it is a placeholder

    // Check that owner drop down field was marked invalid in the css
    expect(element(by.id('owner')).getAttribute('class')).toContain('ng-invalid');

    // Check that error message was printed under owner drop down field
    expect(element(by.cssContainingText('.form-group', 'Owner')).getText()).toMatch(
      'This field is required.'
    );

    // Clicks the Create Bucket button but the page doesn't move. Done by testing
    // for the breadcrumb and heading
    PageHelper.moveClick(element(by.cssContainingText('button', 'Create Bucket'))); // Clicks Create Bucket button
    expect(PageHelper.getTitleText()).toBe('Create Bucket');
    expect(PageHelper.getBreadcrumbText()).toEqual('Create');
    element(by.cssContainingText('button', 'Cancel')).click();

    // content in fields seems to subsist through tests if not cleared, so it is cleared
    nameInputField.clear();
  }

  invalidEdit(name) {
    this.navigateTo();

    browser.wait(Helper.EC.elementToBeClickable(PageHelper.getTableCell(name)), 10000); // wait for table to load
    PageHelper.getTableCell(name).click(); // click on the bucket you want to edit in the table
    element(by.cssContainingText('button', 'Edit')).click(); // click button to move to edit page

    expect(PageHelper.getBreadcrumbText()).toEqual('Edit');

    // Chooses 'Select a user' rather than a valid owner on Edit Bucket page
    // and checks if it's an invalid input
    const ownerDropDown = element(by.id('owner'));
    browser.wait(Helper.EC.elementToBeClickable(ownerDropDown), 5000);

    PageHelper.moveClick(ownerDropDown); // Clicks the Owner drop down on the Create Bucket page
    ownerDropDown
      .all(by.css('option'))
      .first()
      .click(); // select the first option, which is invalid because it is a placeholder

    // Check that owner drop down field was marked invalid in the css
    expect(element(by.id('owner')).getAttribute('class')).toContain('ng-invalid');

    // Check that error message was printed under owner drop down field
    expect(element(by.cssContainingText('.form-group', 'Owner')).getText()).toMatch(
      'This field is required.'
    );

    // Clicks the Edit Bucket button but the page doesn't move. Done by testing for
    // breadcrumb and heading
    expect(element(by.cssContainingText('button', 'Edit Bucket')).getText()).toEqual('Edit Bucket');
    element(by.cssContainingText('button', 'Edit Bucket')).click(); // Gets the Edit button and clicks it
    expect(PageHelper.getBreadcrumbText()).toEqual('Edit');
    expect(PageHelper.getTitleText()).toBe('Edit Bucket');
  }
}
