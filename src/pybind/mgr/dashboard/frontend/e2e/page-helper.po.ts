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
import { Helper } from './helper.po';

const EC = browser.ExpectedConditions;

interface Pages {
  index: string;
}

export abstract class PageHelper {
  pages: Pages;

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
   * This is a decorator to be used on methods which change the current page once, like `navigateTo`
   * and `navigateBack` in this class do. It ensures that, if the new page contains a table, its
   * data has been fully loaded. If no table is detected, it will return instantly.
   */
  static waitForTableData(): Function {
    return (_target: any, _propertyKey: string, descriptor: PropertyDescriptor) => {
      const fn: Function = descriptor.value;
      descriptor.value = async function(...args) {
        const result = fn.apply(this, args);

        // If a table is on the new page, wait until it has gotten its data.
        const implicitWaitTimeout = (await browser.getProcessedConfig()).implicitWaitTimeout;
        await browser
          .manage()
          .timeouts()
          .implicitlyWait(1000);

        const tableCount = await element.all(by.css('cd-table')).count();
        if (tableCount > 0) {
          const progressBars = element.all(by.css('cd-table datatable-progress'));
          await progressBars.each(async (progressBar) => {
            await browser.wait(EC.not(EC.presenceOf(progressBar)), Helper.TIMEOUT);
          });
        }

        await browser
          .manage()
          .timeouts()
          .implicitlyWait(implicitWaitTimeout);

        return result;
      };
    };
  }

  async getBreadcrumbText(): Promise<string> {
    return $('.breadcrumb-item.active').getText();
  }

  async getTabText(index): Promise<string> {
    return $$('.nav.nav-tabs li')
      .get(index)
      .getText();
  }

  async getTableTotalCount(): Promise<number> {
    return Number(
      (await $$('.datatable-footer-inner .page-count span')
        .filter(async (e) => (await e.getText()).includes('total'))
        .first()
        .getText()).match(/.*(\d+\s+)total.*/)[1]
    );
  }

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

  @PageHelper.waitForTableData()
  async navigateTo(page = null) {
    page = page || 'index';
    const url = this.pages[page];
    await browser.get(url);
  }

  @PageHelper.waitForTableData()
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
}
