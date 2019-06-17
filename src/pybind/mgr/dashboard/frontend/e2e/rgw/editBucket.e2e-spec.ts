import { $, browser, protractor } from 'protractor';
import { Helper } from '../helper.po';
import { BucketsCreatePage, BucketsPage, EditBucketPage } from './editBucket.po';

describe('Edit: Create Bucket', () => {
  let page: BucketsCreatePage;

  beforeAll(() => {
    page = new BucketsCreatePage();
  });

  it('should create bucket to test editing feature', () => {
    // Checks if we are on the right page by checking the heading and breadcrumb
    page.navigateTo();
    expect(page.getCreateHeading()).toEqual('Create Bucket');
    expect(Helper.getBreadcrumbText()).toEqual('Create');

    // Types in the name of the bucket (which is 'editBucket'), checks we are
    // still on the Create page
    page.getNameBox().click();
    page.getNameBox().sendKeys('editbucket');
    expect(Helper.getBreadcrumbText()).toEqual('Create');

    // Sets the owner of the bucket (which is 'testid') and checks if the owner
    // was set. Also checks if we are on the Create page
    page.getOwnerBox().click();
    $('[value="testid"]').click();
    expect(page.getOwnerBox().getAttribute('class')).toContain('ng-valid');
    expect(Helper.getBreadcrumbText()).toEqual('Create');

    // Clicks the Create button and checks if its the correct button
    expect(page.getCreateButton().getText()).toEqual('Create Bucket');
    page.getCreateButton().click();
    browser.sleep(2000);
  });
});

describe('Edit checks buckets correct', () => {
  let page: BucketsPage;

  beforeAll(() => {
    page = new BucketsPage();
  });

  describe('Checks that the buckets name, owner, and count is correct', () => {
    beforeEach(() => {
      page.navigateTo();
    });

    it('should check the name of the bucket is correct', () => {
      // Makes sure that the buckets in the table are clickable, then checks if
      // editBucket is in the list
      page.navigateTo();
      const EC = protractor.ExpectedConditions;
      const btable = page.getBucketTable();
      browser.wait(EC.elementToBeClickable(btable.first()), 5000);
      btable.getText().then(function(bucketList) {
        expect(bucketList).toMatch('editbucket');
      });

      // Checks if the owner of the bucket is correct
      page.navigateTo();
      page
        .getBucketTable()
        .getText()
        .then(function(bucketList) {
          expect(bucketList).toMatch('testid');
        });
    });
  });
});

describe('Edits the bucket', () => {
  let page: EditBucketPage;

  beforeAll(() => {
    page = new EditBucketPage();
  });

  describe('Checks heading and breadcrumb, changes owner, and tests cancel button', () => {
    beforeEach(() => {
      page.navigateTo();
    });

    it('should veryify the breadcrumb and heading', () => {
      // Checks that breadcrumb is Edit and the heading is
      // Edit Bucket
      page.navigateTo();
      expect(page.getEditHeading()).toEqual('Edit Bucket');
      expect(Helper.getBreadcrumbText()).toEqual('Edit');
    });

    it('should change the owner of the bucket', () => {
      // Changes owner from 'testid' to 'dev'
      page.navigateTo();
      page.getOwnerBox().click();
      $('[value="dev"]').click();
      expect(page.getOwnerBox().getAttribute('class')).toContain('ng-valid');
    });

    it('should click Edit Bucket button', () => {
      // Clicks the 'Edit Bucket' button and checks if its the correct button
      page.navigateTo();
      expect(page.getEditButton().getText()).toEqual('Edit Bucket');
      page.getEditButton().click();
      browser.sleep(2000);
    });

    it('should verify the cancel button works', () => {
      // Checks if we are brought back to the Buckets page if we cancel
      // editing the bucket
      page.navigateTo();
      Helper.getBreadcrumbText().then(function() {});
      page.getCancelEdit().click();
      const EC = protractor.ExpectedConditions;
      const bcrumb = $('.breadcrumb-item.active');
      browser.wait(EC.visibilityOf(bcrumb), 5000);
      expect(Helper.getBreadcrumbText()).toEqual('Buckets');
    });
  });
});

describe('Checks that edit to bucket owner was made', () => {
  let page: BucketsPage;

  beforeAll(() => {
    page = new BucketsPage();
  });

  describe('Checks that the buckets name, owner, and count is correct', () => {
    beforeEach(() => {
      page.navigateTo();
    });

    it('should check the name and owner of the bucket are correct', () => {
      // Makes sure that the buckets in the table are clickable, then checks if
      // editBucket is in the list
      page.navigateTo();
      const EC = protractor.ExpectedConditions;
      const btable = page.getBucketTable();
      browser.wait(EC.elementToBeClickable(btable.first()), 5000);
      btable.getText().then(function(bucketList) {
        expect(bucketList).toMatch('editbucket');
      });

      // Checks if the owner of the bucket is correct
      page.navigateTo();
      page
        .getBucketTable()
        .getText()
        .then(function(bucketList) {
          expect(bucketList).toMatch('dev');
        });
    });
  });
});

describe('Deletes edited bucket', () => {
  let page: BucketsPage;

  beforeAll(() => {
    page = new BucketsPage();
  });

  describe('Deleting edited bucket to test the edit feature', () => {
    beforeEach(() => {
      page.navigateTo();
    });

    it('should select the edited bucket and deletes it', () => {
      // Checks if the buttons required to delete the buckets are present and if
      // the pop-up menu is present, then clicks the appropriate buttons to delete
      // the bucket. Also checks if the bucket has been deleted by comparing the
      // number of buckets before and after the delete as well as checking that
      // 'editBucket' was present before and after the delete
      page.navigateTo();
      const EC = protractor.ExpectedConditions;
      let numDigits = 1;
      const allb = page.getAllBuckets();
      let count1 = '0';
      const toggle = page.getToggleMenu();
      const dtitle = page.getBoxTitle();
      const confirm = page.getConfirm();
      const delbutton = page.getDelButton();
      let count2 = '0';

      // Gets the number of digits of the number of buckets for the purpose of
      // getting the bucket count from the dashboard
      browser.wait(EC.elementToBeClickable(allb.first()), 5000);
      allb.count().then(function(bcount) {
        numDigits = bcount / 10 + 1;
      });

      // Gets the number of buckets before the deletion of 'editBucket'
      page
        .getBucketCount()
        .getText()
        .then(function(char) {
          count1 = char.substr(13, numDigits);
        });

      // Clicks 'editBucket' in the table
      page.getBucket().click();

      // Clicks the drop down menu and then clicks 'Delete'
      expect(toggle.isPresent()).toBe(true);
      toggle.click();
      page.getDel().click();

      // Checks that pop-up menu is present by checking for the heading
      browser.wait(EC.visibilityOf(dtitle), 5000);
      dtitle.getText().then(function(title) {
        expect(title).toMatch('Delete bucket');
      });

      // Clicking the checkbox and clicking the 'Delete bucket'
      confirm.click();
      browser.wait(EC.elementToBeClickable(delbutton), 5000);
      delbutton.click();

      // Checks that the number of buckets has decreased and checks that the name
      // 'editBucket' is not present
      browser
        .wait(
          function() {
            return page
              .getBucketCount()
              .getText()
              .then(function(c) {
                count2 = c.substr(13, numDigits);
                return count2 < count1;
              });
          },
          5000,
          'Number of buckets never decreased. Deletion failed.'
        )
        .then(function() {
          page
            .getBucketTable()
            .getText()
            .then(function(txt) {
              expect(txt).not.toMatch('editBucket');
            });
        });
    });
  });
});
