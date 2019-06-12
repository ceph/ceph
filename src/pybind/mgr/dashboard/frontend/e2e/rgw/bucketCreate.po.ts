
import { $, browser, by, element } from 'protractor';

export class BucketsPage {
  navigateTo() {
    return browser.get('/#/rgw/bucket');
  }
  getBucketTable() {
    return element.all(by.css('.datatable-body'));
  }
  getAllBuckets() {
   return element.all(by.css('div.datatable-body-cell-label'));
  }
  getBucket() {
    return element.all(by.css('div.datatable-body-cell-label')).filter(function(elem, index) {
      return elem.getText().then(function(text) {
        const i = index;
        index = i;
        return text === 'testbucket';
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
  getBucketCount() {
    return $('.datatable-footer-inner.selected-count');
  }
  getCreateABucket() {
    return element.all(by.css('button.btn.btn-sm.btn-primary')).first();
  }

}

export class BucketsCreatePage {
  navigateTo() {
    return browser.get('/#/rgw/bucket/create');
  }
  getCreateHeading() {
    return $('.panel-title').getText();
  }
  getNameBox() {
    return element(by.id('bid'));
  }
  getOwnerBox() {
    return element(by.id('owner'));
  }
  getCreateButton() {
    return $('.btn.btn-sm.btn-primary.tc_submitButton');
  }
  getCancelBucket() {
    return element(by.buttonText('Cancel'));
  }
}
