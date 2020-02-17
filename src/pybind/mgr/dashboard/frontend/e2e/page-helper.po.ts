import {
  $,
  $$,
  browser,
  by,
  element,
  ElementArrayFinder,
  ElementFinder,
  protractor
} from 'protractor';

const EC = browser.ExpectedConditions;
const TIMEOUT = 20000;

interface Pages {
  index: string;
}

export abstract class PageHelper {
  pages: Pages;

  /**
   * Checks if there are any errors on the browser
   *
   * @static
   * @memberof Helper
   */
  static async checkConsole() {
    let browserLog = await browser
      .manage()
      .logs()
      .get('browser');

    browserLog = browserLog.filter((log) => log.level.value > 900);

    if (browserLog.length > 0) {
      console.log('\n log: ' + require('util').inspect(browserLog));
    }

    await expect(browserLog.length).toEqual(0);
  }

  /**
   * Decorator to be used on Helper methods to restrict access to one particular URL.  This shall
   * help developers to prevent and highlight mistakes.  It also reduces boilerplate code and by
   * thus, increases readability.
   */
  static restrictTo(page): Function {
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

  /**
   * Get the active breadcrumb item.
   */
  getBreadcrumb(): ElementFinder {
    return $('.breadcrumb-item.active');
  }

  async getTabText(index): Promise<string> {
    return $$('.nav.nav-tabs li')
      .get(index)
      .getText();
  }

  async getTableTotalCount(): Promise<number> {
    const text = await $$('.datatable-footer-inner .page-count span')
      .filter(async (e) => (await e.getText()).includes('total'))
      .first()
      .getText();
    return Number(text.match(/(\d+)\s+total/)[1]);
  }

  async getTableSelectedCount(): Promise<number> {
    const text = await $$('.datatable-footer-inner .page-count span')
      .filter(async (e) => (await e.getText()).includes('selected'))
      .first()
      .getText();
    return Number(text.match(/(\d+)\s+selected/)[1]);
  }

  getFirstTableCellWithText(content: string): ElementFinder {
    return element.all(by.cssContainingText('.datatable-body-cell-label', content)).first();
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

  /**
   * Ceph Dashboards' <input type="checkbox"> tag is not visible. Instead of the real checkbox, a
   * replacement is shown which is supposed to have an adapted style. The replacement checkbox shown
   * is part of the label and is rendered in the "::before" pseudo element of the label, hence the
   * label is always clicked when the user clicks the replacement checkbox.
   *
   * This method finds corresponding label to the given checkbox and clicks it instead of the (fake)
   * checkbox, like it is the case with real users.
   *
   * Alternatively, the checkbox' label can be passed.
   *
   * @param elem The checkbox or corresponding label
   */
  async clickCheckbox(elem: ElementFinder): Promise<void> {
    const tagName = await elem.getTagName();
    let label: ElementFinder = null; // Both types are clickable

    await this.waitPresence(elem);
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

    return this.waitClickableAndClick(label);
  }

  /**
   * Returns the cell with the content given in `content`. Will not return a rejected Promise if the
   * table cell hasn't been found. It behaves this way to enable to wait for
   * visibility/invisibility/presence of the returned element.
   *
   * It will return a rejected Promise if the result is ambiguous, though. That means if the search
   * for content has been completed, but more than a single row is shown in the data table.
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
      return Promise.reject('getTableCellByContent: Result is ambiguous');
    } else {
      return Promise.resolve(
        element(
          by.cssContainingText('.datatable-body-cell-label', new RegExp(`^\\s${content}\\s$`))
        )
      );
    }
  }

  /**
   * Used when .clear() does not work on a text box, sends a Ctrl + a, BACKSPACE
   */
  async clearInput(elem: ElementFinder) {
    const types = ['text', 'number'];
    if ((await elem.getTagName()) === 'input' && types.includes(await elem.getAttribute('type'))) {
      await elem.sendKeys(
        protractor.Key.chord(protractor.Key.CONTROL, 'a'),
        protractor.Key.BACK_SPACE
      );
    } else {
      return Promise.reject(`Element ${elem} does not match the expected criteria.`);
    }
  }

  async navigateTo(page = null) {
    page = page || 'index';
    const url = this.pages[page];
    await browser.get(url);
  }

  async navigateBack() {
    await browser.navigate().back();
  }

  getDataTables(): ElementArrayFinder {
    return $$('cd-table');
  }

  /**
   * Gets column headers of table
   */
  getDataTableHeaders(): ElementArrayFinder {
    return $$('.datatable-header');
  }

  /**
   * Grabs striped tables
   */
  getStatusTables(): ElementArrayFinder {
    return $$('.table.table-striped');
  }

  /**
   * Grabs legends above tables
   */
  getLegends(): ElementArrayFinder {
    return $$('legend');
  }

  async waitPresence(elem: ElementFinder, message?: string) {
    return browser.wait(EC.presenceOf(elem), TIMEOUT, message);
  }

  async waitStaleness(elem: ElementFinder, message?: string) {
    return browser.wait(EC.stalenessOf(elem), TIMEOUT, message);
  }

  /**
   * This method will wait for the element to be clickable and then click it.
   */
  async waitClickableAndClick(elem: ElementFinder, message?: string) {
    await browser.wait(EC.elementToBeClickable(elem), TIMEOUT, message);
    return elem.click();
  }

  async waitVisibility(elem: ElementFinder, message?: string) {
    return browser.wait(EC.visibilityOf(elem), TIMEOUT, message);
  }

  async waitInvisibility(elem: ElementFinder, message?: string) {
    return browser.wait(EC.invisibilityOf(elem), TIMEOUT, message);
  }

  async waitTextToBePresent(elem: ElementFinder, text: string, message?: string) {
    return browser.wait(EC.textToBePresentInElement(elem, text), TIMEOUT, message);
  }

  async waitTextNotPresent(elem: ElementFinder, text: string, message?: string) {
    return browser.wait(EC.not(EC.textToBePresentInElement(elem, text)), TIMEOUT, message);
  }

  async waitFn(func: Function, message?: string) {
    return browser.wait(func, TIMEOUT, message);
  }

  getFirstCell(): ElementFinder {
    return $$('.datatable-body-cell-label').first();
  }

  /**
   * This is a generic method to delete table rows.
   * It will select the first row that contains the provided name and delete it.
   * After that it will wait until the row is no longer displayed.
   */
  async delete(name: string): Promise<any> {
    // Selects row
    await this.waitClickableAndClick(this.getFirstTableCellWithText(name));

    // Clicks on table Delete button
    await $$('.table-actions button.dropdown-toggle')
      .first()
      .click(); // open submenu
    await $('li.delete a').click(); // click on "delete" menu item

    // Confirms deletion
    await this.clickCheckbox($('.custom-control-label'));
    await element(by.cssContainingText('button', 'Delete')).click();

    // Waits for item to be removed from table
    return this.waitStaleness(this.getFirstTableCellWithText(name));
  }

  getTableRows() {
    return $$('datatable-row-wrapper');
  }

  /**
   * Uncheck all checked table rows.
   */
  async uncheckAllTableRows() {
    await $$('.datatable-body-cell-label .datatable-checkbox input[type=checkbox]:checked').each(
      (e: ElementFinder) => e.click()
    );
  }
}
