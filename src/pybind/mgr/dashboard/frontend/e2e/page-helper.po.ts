import { $, $$, browser, by, element, ElementFinder, protractor } from 'protractor';

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
              : Promise.reject(
                  `Method ${target.constructor.name}::${propertyKey} is supposed to be ` +
                    `run on path "${page}", but was run on URL "${url}"`
                )
          );
      };
    };
  }

  async getBreadcrumbText(): Promise<string> {
    return $('.breadcrumb-item.active').getText();
  }

  async getTabText(idx): Promise<string> {
    return $$('.nav.nav-tabs li')
      .get(idx)
      .getText();
  }
  //
  // getTableCount() {
  //   return $('.datatable-footer-inner.selected-count');
  // }
  //
  // getTitleText() {
  //   let title;
  //   return browser
  //     .wait(() => {
  //       title = $('.panel-title');
  //       return title.isPresent();
  //     })
  //     .then(() => title.getText());
  // }
  //
  getTableCell(content: string): ElementFinder {
    return element(by.cssContainingText('.datatable-body-cell-label', content));
  }

  getTableRow(content) {
    return element(by.cssContainingText('.datatable-body-row', content));
  }

  getTable(): ElementFinder {
    return $('.datatable-body');
  }

  async getTabsCount(): Promise<number> {
    return $$('.nav.nav-tabs li').count();
  }

  getFirstTableCellWithText(content): ElementFinder {
    return element.all(by.cssContainingText('.datatable-body-cell-label', content)).first();
  }

  /**
   * Used for instances where a modal container would receive the click rather
   * than the desired element.
   *
   * Our <input type="checkbox"> tag is not visible. Instead of the real
   * checkbox, a replacement is shown which is supposed to have an adapted
   * style. The replacement checkbox shown is part of the label and is rendered
   * in the "::before" pseudo element of the label, hence the label is always
   * clicked when the user clicks the replacement checkbox.
   *
   * This method finds corresponding label to the given checkbox and clicks it
   * instead of the (fake) checkbox, like it is the case with real users.
   *
   * Alternatively, the checkbox' label can be passed.
   *
   * @param elem The checkbox or corresponding label
   */
  async clickCheckbox(elem: ElementFinder): Promise<void> {
    const tagName = await elem.getTagName();
    let label: ElementFinder = null; // Both types are clickable

    if (tagName === 'input') {
      if ((await elem.getAttribute('type')) === 'checkbox') {
        label = elem.element(by.xpath('..')).$(`label[for="${await elem.getAttribute('id')}"]`);
      } else {
        return Promise.reject('element <input> must be of type checkbox');
      }
    } else if (tagName === 'label') {
      label = elem;
    } else {
      return Promise.reject(
        `element <${tagName}> is not of the correct type. You need to pass a checkbox or label`
      );
    }

    return label.click();
  }

  /**
   * Returns the cell with the content given in `content`. Will not return a
   * rejected Promise if the table cell hasn't been found. It behaves this way
   * to enable to wait for visiblity/invisiblity/presence of the returned
   * element.
   *
   * It will return a rejected Promise if the result is ambigous, though. That
   * means if the search for content has been completed, but more than a single
   * row is shown in the data table.
   */
  async getTableCellByContent(content: string): Promise<ElementFinder> {
    const searchInput = $('#pool-list > div .search input');
    const rowAmountInput = $('#pool-list > div > div > .dataTables_paginate input');
    const footer = $('#pool-list > div datatable-footer');

    await rowAmountInput.clear();
    await rowAmountInput.sendKeys('10');
    await searchInput.clear();
    await searchInput.sendKeys(content);

    const count = Number(await footer.getAttribute('ng-reflect-row-count'));
    if (count !== 0 && count > 1) {
      return Promise.reject('getTableCellByContent: Result is ambigous');
    } else {
      return Promise.resolve(
        element(
          by.cssContainingText('.datatable-body-cell-label', new RegExp(`^\\s${content}\\s$`))
        )
      );
    }
  }

  // used when .clear() does not work on a text box, sends a Ctrl + a, BACKSPACE
  inputClear(elem) {
    elem.sendKeys(protractor.Key.chord(protractor.Key.CONTROL, 'a'));
    elem.sendKeys(protractor.Key.BACK_SPACE);
  }

  async navigateTo(page = null) {
    page = page || 'index';
    const url = this.pages[page];
    return browser.get(url);
  }
  //
  // getDataTable() {
  //   return $$('cd-table');
  // }
  //
  // getStatusTable() {
  //   // Grabs striped tables
  //   return $$('.table.table-striped');
  // }
  //
  // getLegends() {
  //   // Grabs legends above tables
  //   return $$('legend');
  // }
  //
  // getDataTableHeaders() {
  //   // Gets column headers of table
  //   return $$('.datatable-header');
  // }
}
