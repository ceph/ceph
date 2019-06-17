import { $, browser, by, element } from 'protractor';

export class UserPage {
  navigateTo() {
    return browser.get('/#/rgw/user');
  }

  getUserTable() {
    return element.all(by.css('.datatable-body'));
  }

  getAllUsers() {
    return element.all(by.css('div.datatable-body-cell-label'));
  }

  getUser() {
    return element.all(by.css('div.datatable-body-cell-label')).filter(function(elem, index) {
      return elem.getText().then(function(text) {
        const i = index;
        index = i;
        return text === '000user';
      });
    });
  }

  getToggleMenu() {
    return element(by.css('.btn.btn-sm.btn-primary.dropdown-toggle.dropdown-toggle-split'));
  }

  getDel() {
    return element(by.css('.delete'));
  }

  getConfirm() {
    return element.all(by.css('.ng-untouched.ng-pristine.ng-invalid')).last();
  }

  getDelButton() {
    return element(by.css('.btn.btn-sm.btn-primary.tc_submitButton'));
  }

  getBoxTitle() {
    return element(by.css('.modal-title.pull-left'));
  }

  getUserCount() {
    return element.all(by.css('.datatable-footer-inner.selected-count')).first();
  }

  getCreateUser() {
    return element.all(by.css('button.btn.btn-sm.btn-primary')).first();
  }
}

export class UserCreatePage {
  navigateTo() {
    return browser.get('/#/rgw/user/create');
  }

  getCreateHeading() {
    return $('.panel-title').getText();
  }

  getNameBox() {
    return element(by.id('uid'));
  }

  getFullNameBox() {
    return element(by.id('display_name'));
  }

  getEmailBox() {
    return element(by.id('email'));
  }

  getCreateButton() {
    return $('.btn.btn-sm.btn-primary.tc_submitButton');
  }

  getCancel() {
    return element(by.buttonText('Cancel'));
  }
}
