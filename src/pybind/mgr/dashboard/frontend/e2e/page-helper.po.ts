import { $, $$, browser, by, element, ElementFinder, promise } from 'protractor';

interface Pages {
  index: string;
}

export abstract class PageHelper {
  pages: Pages;

  /**
   * Decorator to be used on Helper methods to restrict access to one
   * particular URL.  This shall help developers to prevent and highlight
   * mistakes.  It also reduces boilerplate code and by thus, increases
   * readability.
   */
  static restrictTo(page): any {
    return (target: any, propertyKey: string, descriptor: PropertyDescriptor) => {
      const fn: Function = descriptor.value;
      descriptor.value = function(...args) {
        return browser
          .getCurrentUrl()
          .then((url) =>
            url.endsWith(page)
              ? fn.apply(this, args)
              : promise.Promise.reject(
                  `Method ${target.constructor.name}::${propertyKey} is supposed to be ` +
                    `run on path "${page}", but was run on URL "${url}"`
                )
          );
      };
    };
  }

  getBreadcrumbText() {
    return $('.breadcrumb-item.active').getText();
  }

  getTabText(idx) {
    return $$('.nav.nav-tabs li')
      .get(idx)
      .getText();
  }

  getTableCount() {
    return $('.datatable-footer-inner.selected-count');
  }

  getTitleText() {
    let title;
    return browser
      .wait(() => {
        title = $('.panel-title');
        return title.isPresent();
      })
      .then(() => title.getText());
  }

  getTableCell(content) {
    return element(by.cssContainingText('.datatable-body-cell-label', content));
  }

  getTable() {
    return element.all(by.css('.datatable-body'));
  }

  getTabsCount() {
    return $$('.nav.nav-tabs li').count();
  }

  getFirstTableCellWithText(content) {
    return element.all(by.cssContainingText('.datatable-body-cell-label', content)).first();
  }

  /**
   * Used for instances where a modal container received the click rather than the desired element.
   *
   * https://stackoverflow.com/questions/26211751/protractor-chrome-driver-element-is-not-clickable-at-point
   */
  moveClick(object) {
    return browser
      .actions()
      .mouseMove(object)
      .click()
      .perform();
  }

  /**
   * Returns the cell with the content given in `content`. Will not return a
   * rejected Promise if the table cell hasn't been found. It behaves this way
   * to enable to wait for visiblity/invisiblity/precense of the returned
   * element.
   *
   * It will return a rejected Promise if the result is ambigous, though. That
   * means if after the search for content has been completed, but more than a
   * single row is shown in the data table.
   */
  getTableCellByContent(content: string): promise.Promise<ElementFinder> {
    const searchInput = $('#pool-list > div .search input');
    const rowAmountInput = $('#pool-list > div > div > .dataTables_paginate input');
    const footer = $('#pool-list > div datatable-footer');

    rowAmountInput.clear();
    rowAmountInput.sendKeys('10');
    searchInput.clear();
    searchInput.sendKeys(content);

    return footer.getAttribute('ng-reflect-row-count').then((rowCount: string) => {
      const count = Number(rowCount);
      if (count !== 0 && count > 1) {
        return Promise.reject('getTableCellByContent: Result is ambigous');
      } else {
        return element(
          by.cssContainingText('.datatable-body-cell-label', new RegExp(`^\\s${content}\\s$`))
        );
      }
    });
  }

  navigateTo(page = null) {
    page = page || 'index';
    const url = this.pages[page];
    return browser.get(url);
  }

  getDataTable() {
    return $$('cd-table');
  }

  getStatusTable() {
    // Grabs striped tables
    return $$('.table.table-striped');
  }

  getLegends() {
    // Grabs legends above tables
    return $$('legend');
  }

  getDataTableHeaders() {
    // Gets column headers of table
    return $$('.datatable-header');
  }
}
