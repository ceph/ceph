import { $, browser, protractor } from 'protractor';
import { Helper } from '../helper.po';
import { BucketsCreatePage, BucketsPage } from './bucketCreate.po';

describe('RGW Buckets Page', () => {
  let page: BucketsPage;

  beforeAll(() => {
    page = new BucketsPage();
  });

  describe('Click Create to go to the Create page', () => {
    beforeEach(() => {
      page.navigateTo();
    });

    it('should click the Create button', () => {
      // Checks the breadcrumb at the Buckets page, clicks the create button and then
      // checks the breadcrumb again to see if we moved to the Create page
      page.navigateTo();
      expect(Helper.getBreadcrumbText()).toEqual('Buckets');
      page.getCreateABucket().click();
      expect(Helper.getBreadcrumbText()).toEqual('Create');
    });
  });
});

describe('Create bucket from the Create page', () => {
  let page: BucketsCreatePage;

  beforeAll(() => {
    page = new BucketsCreatePage();
  });

  describe('Types name, sets owner, and creates bucket', () => {
    beforeEach(() => {
      page.navigateTo();
    });

    it('Heading should be Create Bucket', () => {
      // Checks if we are on the right page by checking the heading and breadcrumb
      page.navigateTo();
      expect(page.getCreateHeading()).toEqual('Create Bucket');
      expect(Helper.getBreadcrumbText()).toEqual('Create');
    });

    it('should type name of bucket', () => {
      // Types in the name of the bucket (which is 'testbucket'), checks we are
      // still on the Create page
      page.navigateTo();
      page.getNameBox().click();
      page.getNameBox().sendKeys('testbucket');
      expect(Helper.getBreadcrumbText()).toEqual('Create');
    });

    it('should set owner', () => {
      // Sets the owner of the bucket (which is 'testid') and checks if the owner
      // was set. Also checks if we are on the Create page
      page.navigateTo();
      page.getOwnerBox().click();
      $('[value="testid"]').click();
      expect(page.getOwnerBox().getAttribute('class')).toContain('ng-valid');
      expect(Helper.getBreadcrumbText()).toEqual('Create');
    });

    it('should click Create Bucket button', () => {
      // Clicks the Create button and checks if its the correct button
      page.navigateTo();
      expect(page.getCreateButton().getText()).toEqual('Create Bucket');
      page.getCreateButton().click();
      browser.sleep(5000);
    });
    it('Cancel Button brings you back to Buckets page', () => {
      // Checks if we are brought back to the Buckets page after creating
      // the bucket
      page.navigateTo();
      Helper.getBreadcrumbText().then(function() {});
      page.getCancelBucket().click();
      const EC = protractor.ExpectedConditions;
      const bcrumb = $('.breadcrumb-item.active');
      browser.wait(EC.visibilityOf(bcrumb), 5000);
      expect(Helper.getBreadcrumbText()).toEqual('Buckets');
    });
  });
});

describe('Checks that bucket was made', () => {
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
      // testbucket is in the list
      page.navigateTo();
      const EC = protractor.ExpectedConditions;
      const btable = page.getBucketTable();
      browser.wait(EC.elementToBeClickable(btable.first()), 5000);
      btable.getText().then(function(bucketList) {
        expect(bucketList).toMatch('testbucket');
      });
    });
    it('Should check the owner of the bucket is correct', () => {
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

describe('Checks that a bucket was deleted', () => {
  let page: BucketsPage;

  beforeAll(() => {
    page = new BucketsPage();
  });

  describe('Selects bucket, clicks dropdown, selects delete, checks box, and deletes ', () => {
    beforeEach(() => {
      page.navigateTo();
    });

    it('should select the bucket and delete it', () => {
      // Checks if the buttons required to delete the buckets are present and if
      // the pop-up menu is present, then clicks the appropriate buttons to delete
      // the bucket. Also checks if the bucket has been deleted by comparing the
      // number of buckets before and after the delete as well as checking that
      // 'testbucket' was present before and after the delete
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

      // Gets the number of buckets before the deletion of 'testbucket'
      page
        .getBucketCount()
        .getText()
        .then(function(char) {
          count1 = char.substr(13, numDigits);
        });

      // Clicks 'testbucket' in the table
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
      // 'testbucket' is not present
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
              expect(txt).not.toMatch('testbucket');
            });
        });
    });
  });
});
