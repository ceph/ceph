import { $, browser, protractor } from 'protractor';
import { Helper } from '../helper.po';
import { UserCreatePage, UserPage } from './userCreate.po';

describe('RGW User Page', () => {
  let page: UserPage;

  beforeAll(() => {
    page = new UserPage();
  });

  describe('Click Create to go to the Create page', () => {
    beforeEach(() => {
      page.navigateTo();
    });

    it('should click the Create button', () => {
      // Checks the breadcrumb at the User page, clicks the create button and then
      // checks the breadcrumb again to see if we moved to the Create page
      page.navigateTo();
      expect(Helper.getBreadcrumbText()).toEqual('Users');
      page.getCreateUser().click();
      expect(Helper.getBreadcrumbText()).toEqual('Create');
    });
  });
});

describe('Create user from the Create page', () => {
  let page: UserCreatePage;

  beforeAll(() => {
    page = new UserCreatePage();
  });

  describe('Sets name, sets full name, sets email and creates user', () => {
    beforeEach(() => {
      page.navigateTo();
    });

    it('should check for proper heading', () => {
      // Checks if we are on the right page by checking the heading and breadcrumb
      page.navigateTo();
      expect(page.getCreateHeading()).toEqual('Create User');
      expect(Helper.getBreadcrumbText()).toEqual('Create');
    });

    it('should type username', () => {
      // Types in the username field
      page.navigateTo();
      page.getNameBox().click();
      page.getNameBox().sendKeys('000user');
      expect(Helper.getBreadcrumbText()).toEqual('Create');
    });

    it('should type full name', () => {
      // Types in the full name field
      page.navigateTo();
      page.getFullNameBox().click();
      page.getFullNameBox().sendKeys('Sample Name');
    });

    it('should type email address', () => {
      // Types in the Email Address field
      page.navigateTo();
      page.getEmailBox().click();
      page.getEmailBox().sendKeys('test@website.com');
    });

    it('should click Create User button', () => {
      // Clicks the Create button and checks if its the correct button
      page.navigateTo();
      expect(page.getCreateButton().getText()).toEqual('Create User');
      page.getCreateButton().click();
      browser.sleep(2000);
    });

    it('should check cancel button brings you back to User page', () => {
      // Checks if we are brought back to the User page after creating
      // the user
      page.navigateTo();
      Helper.getBreadcrumbText().then(function() {});
      page.getCancel().click();
      const EC = protractor.ExpectedConditions;
      const bcrumb = $('.breadcrumb-item.active');
      browser.wait(EC.visibilityOf(bcrumb), 5000);
      expect(Helper.getBreadcrumbText()).toEqual('Users');
    });
  });
});

describe('Checks that user was made', () => {
  let page: UserPage;

  beforeAll(() => {
    page = new UserPage();
  });

  describe('Check that the Users username, full name, and email is correct', () => {
    beforeEach(() => {
      page.navigateTo();
    });

    it('should check the username of the user is present', () => {
      // Makes sure that the users in the table are clickable, then checks if
      // 000user is in the list
      page.navigateTo();
      const EC = protractor.ExpectedConditions;
      const utable = page.getUserTable();
      browser.wait(EC.elementToBeClickable(utable.first()), 5000);
      utable.getText().then(function(userList) {
        expect(userList).toMatch('000user');
      });
    });

    it('should check the full name of the user is present', () => {
      // Checks if the full name of the user is correct
      page.navigateTo();
      page
        .getUserTable()
        .getText()
        .then(function(userList) {
          expect(userList).toMatch('Sample Name');
        });
    });

    it('should check the email of the user is present', () => {
      // Checks if the email of the user is correct
      page.navigateTo();
      page
        .getUserTable()
        .getText()
        .then(function(userList) {
          expect(userList).toMatch('test@website.com');
        });
    });
  });
});

describe('Checks that new user was deleted', () => {
  let page: UserPage;

  beforeAll(() => {
    page = new UserPage();
  });

  describe('Selects user, clicks dropdown, selects delete, checks box, and deletes ', () => {
    beforeEach(() => {
      page.navigateTo();
    });

    it('should select the new user and delete it', () => {
      // Checks if the buttons required to delete the users are present and if
      // the pop-up menu is present, then clicks the appropriate buttons to delete
      // the selected user. Also checks if the user has been deleted by comparing the
      // number of users before and after the delete as well as checking that
      // 'testuser' is no longer present after the delete
      page.navigateTo();
      const EC = protractor.ExpectedConditions;
      let numDigits = 1;
      const allu = page.getAllUsers();
      let count1 = '0';
      const toggle = page.getToggleMenu();
      const dtitle = page.getBoxTitle();
      const confirm = page.getConfirm();
      const delbutton = page.getDelButton();
      let count2 = '0';

      // Gets the number of digits of the number of users for the purpose of
      // getting the user count from the dashboard
      browser.wait(EC.elementToBeClickable(allu.first()), 5000);
      allu.count().then(function(ucount) {
        numDigits = ucount / 10 + 1;
      });

      // Gets the number of users before the deletion of '000user'
      page
        .getUserCount()
        .getText()
        .then(function(char) {
          count1 = char.substr(13, numDigits);
        });

      // Clicks '000user' in the table
      page.getUser().click();

      // Clicks the drop down menu and then clicks 'Delete'
      expect(toggle.isPresent()).toBe(true);
      toggle.click();
      page.getDel().click();

      // Checks that pop-up menu is present by checking for the heading
      browser.wait(EC.visibilityOf(dtitle), 5000);
      dtitle.getText().then(function(title) {
        expect(title).toMatch('Delete user');
      });

      // Clicking the checkbox and clicking the 'Delete user'
      confirm.click();
      browser.wait(EC.elementToBeClickable(delbutton), 5000);
      delbutton.click();

      // Checks that the number of users has decreased and checks that the username
      // '000user' is not present
      browser
        .wait(
          function() {
            return page
              .getUserCount()
              .getText()
              .then(function(totalUsers) {
                count2 = totalUsers.substr(13, numDigits);
                return count2 < count1;
              });
          },
          5000,
          'Number of users never decreased. Deletion failed.'
        )
        .then(function() {
          page
            .getUserTable()
            .getText()
            .then(function(txt) {
              expect(txt).not.toMatch('000user');
            });
        });
    });
  });
});
