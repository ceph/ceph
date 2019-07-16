import { $, $$, browser, by, element } from 'protractor';

interface Pages {
  index: string;
}

export abstract class PageHelper {
  pages: Pages;

  static getBreadcrumbText() {
    return $('.breadcrumb-item.active').getText();
  }

  static getTabText(idx) {
    return $$('.nav.nav-tabs li')
      .get(idx)
      .getText();
  }

  static getTableCount() {
    return $('.datatable-footer-inner.selected-count');
  }

  static getTitleText() {
    let title;
    return browser
      .wait(() => {
        title = $('.panel-title');
        return title.isPresent();
      })
      .then(() => title.getText());
  }

  static getTableCell(content) {
    return element(by.cssContainingText('.datatable-body-cell-label', content));
  }

  static getTable() {
    return element.all(by.css('.datatable-body'));
  }

  static getTabsCount() {
    return $$('.nav.nav-tabs li').count();
  }

  static getFirstTableCellWithText(content) {
    return element.all(by.cssContainingText('.datatable-body-cell-label', content)).first();
  }

  // Used for instances where a modal container recieved the click rather than the
  // desired element
  static moveClick(object) {
    return browser
      .actions()
      .mouseMove(object)
      .click()
      .perform();
  }

  navigateTo(page = null) {
    page = page || 'index';
    const url = this.pages[page];
    return browser.get(url);
  }
}
