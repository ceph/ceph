interface Page {
  url: string;
  id: string;
}

export abstract class PageHelper {
  pages: Record<string, Page>;

  /**
   * Decorator to be used on Helper methods to restrict access to one particular URL.  This shall
   * help developers to prevent and highlight mistakes.  It also reduces boilerplate code and by
   * thus, increases readability.
   */
  static restrictTo(page: string): Function {
    return (target: any, propertyKey: string, descriptor: PropertyDescriptor) => {
      if (descriptor) {
        const fn: Function = descriptor.value;
        descriptor.value = function (...args: any) {
          cy.location('hash').should((url) => {
            expect(url).to.eq(
              page,
              `Method ${target.constructor.name}::${propertyKey} is supposed to be ` +
                `run on path "${page}", but was run on URL "${url}"`
            );
          });
          fn.apply(this, args);
        };
      }
    };
  }

  /**
   * Navigates to the given page or to index.
   * Waits until the page component is loaded
   */
  navigateTo(name: string = null) {
    name = name || 'index';
    const page = this.pages[name];

    cy.visit(page.url);
    cy.get(page.id);
  }

  /**
   * Navigates back and waits for the hash to change
   */
  navigateBack() {
    cy.location('hash').then((hash) => {
      cy.go('back');
      cy.location('hash').should('not.be', hash);
    });
  }

  /**
   * Navigates to the edit page
   */
  navigateEdit(
    name: string,
    _select = true,
    breadcrumb = true,
    navigateTo: string = null,
    isMultiselect = false
  ) {
    if (navigateTo) {
      this.navigateTo(navigateTo);
    } else if (isMultiselect) {
      this.clickActionButtonFromMultiselect(name);
      cy.contains('Creating...').should('not.exist');
      cy.contains('button', 'Edit').click();
    } else {
      this.clickRowActionButton(name, 'edit');
    }
    if (breadcrumb) {
      this.expectBreadcrumbText('Edit');
    }
  }

  /**
   * Checks the active breadcrumb value.
   */
  expectBreadcrumbText(text: string) {
    cy.get('[data-testid="active-breadcrumb-item"]')
      .last()
      .invoke('text')
      .then((crumb) => {
        expect(crumb.trim()).to.equal(text);
      });
  }

  getTabs() {
    return cy.get('.nav.nav-tabs a');
  }

  getTab(tabName: string) {
    return cy.contains('.nav.nav-tabs a', tabName);
  }

  getTabText(index: number) {
    return this.getTabs().its(index).text();
  }

  getTabsCount(): any {
    return this.getTabs().its('length');
  }

  /**
   * Helper method to navigate/click a tab inside the expanded table row.
   * @param selector The selector of the expanded table row.
   * @param name The name of the row which should expand.
   * @param tabName Name of the tab to be navigated/clicked.
   */
  clickTab(selector: string, name: string, tabName: string) {
    this.getExpandCollapseElement(name).click();
    cy.get(selector).within(() => {
      this.getTab(tabName).click();
    });
  }

  /**
   * Helper method to select an option inside a select element.
   * This method will also expect that the option was set.
   * @param option The option text (not value) to be selected.
   */
  selectOption(selectionName: string, option: string) {
    cy.get(`select[id=${selectionName}]`).select(option);
    return this.expectSelectOption(selectionName, option);
  }

  /**
   * Helper method to expect a set option inside a select element.
   * @param option The selected option text (not value) that is to
   *   be expected.
   */
  expectSelectOption(selectionName: string, option: string) {
    return cy.get(`select[id=${selectionName}] option:checked`).contains(option);
  }

  getLegends() {
    return cy.get('legend');
  }

  getToast() {
    return cy.get('.ngx-toastr');
  }

  /**
   * Waits for the table to load its data
   * Should be used in all methods that access the datatable
   */
  private waitDataTableToLoad() {
    cy.get('cd-table').should('exist');
    cy.get('table[cdstable] tbody').should('exist');
    cy.contains('Loading').should('not.exist');
  }

  getDataTables() {
    this.waitDataTableToLoad();

    return cy.get('cd-table [cdsTable]');
  }

  private getTableCountSpan(_spanType: 'selected' | 'found' | 'total' | 'item' | 'items') {
    return cy.contains('.cds--pagination__text.cds--pagination__items-count', /item|items/gi);
  }

  // Get 'selected', 'found', or 'total' row count of a table.
  getTableCount(spanType: 'selected' | 'found' | 'total' | 'item' | 'items') {
    this.waitDataTableToLoad();
    cy.wait(1 * 1000);
    return this.getTableCountSpan(spanType).then(($elem) => {
      const text = $elem.first().text();
      return Number(text.match(/\b\d+(?= item|items\b)/)[0]);
    });
  }

  // Wait until selected', 'found', or 'total' row count of a table equal to a number.
  expectTableCount(spanType: 'selected' | 'found' | 'total' | 'item' | 'items', count: number) {
    this.waitDataTableToLoad();
    this.getTableCountSpan(spanType).should(($elem) => {
      const text = $elem.first().text();
      expect(Number(text.match(/\b\d+(?= item|items\b)/)[0])).to.equal(count);
    });
  }

  getTableRow(content: string) {
    this.waitDataTableToLoad();

    this.searchTable(content);
    return cy.contains('[cdstablerow]', content);
  }

  getTableRows() {
    this.waitDataTableToLoad();

    return cy.get('[cdstablerow]');
  }

  /**
   * Returns the first table cell.
   * Optionally, you can specify the content of the cell.
   */
  getFirstTableCell(content?: string) {
    this.waitDataTableToLoad();

    if (content) {
      this.searchTable(content);
      return cy.contains('[cdstablerow] [cdstabledata]', content);
    } else {
      return cy.get('[cdstablerow] [cdstabledata]').first();
    }
  }

  getTableCell(columnIndex: number, exactContent: string, partialMatch = false) {
    this.waitDataTableToLoad();
    this.clearTableSearchInput();
    cy.wait(1 * 1000);
    this.searchTable(exactContent);
    if (partialMatch) {
      return cy.contains(`[cdstablerow] [cdstabledata]:nth-child(${columnIndex})`, exactContent);
    }
    return cy.contains(
      `[cdstablerow] [cdstabledata]:nth-child(${columnIndex})`,
      new RegExp(`^${exactContent}$`)
    );
  }

  existTableCell(name: string, oughtToBePresent = true) {
    const waitRule = oughtToBePresent ? 'be.visible' : 'not.exist';
    this.getFirstTableCell(name).should(waitRule);
  }

  getExpandCollapseElement(content?: string) {
    this.waitDataTableToLoad();
    if (content) {
      return cy
        .contains('[cdstablerow] [cdstabledata]', content)
        .parent('[cdstablerow]')
        .find('[cdstableexpandbutton] .cds--table-expand__button');
    }
    return cy.get('.cds--table-expand__button').first();
  }

  /**
   * Gets column headers of table
   */
  getDataTableHeaders() {
    this.waitDataTableToLoad();

    return cy.get('[cdstableheadcell]');
  }

  /**
   * Grabs striped tables
   */
  getStatusTables() {
    return cy.get(
      '.cds--data-table--sort.cds--data-table--no-border.cds--data-table.cds--data-table--md'
    );
  }

  filterTable(name: string, option: string) {
    this.waitDataTableToLoad();
    cy.get('select#filter_name').select(name);
    cy.get('select#filter_option').select(option);
  }

  setPageSize(size: string) {
    cy.get('.cds--select__item-count .cds--select-input').select(size, { force: true });
  }

  searchTable(text: string, delay = 35) {
    this.waitDataTableToLoad();

    cy.get('.cds--search-input').first().clear({ force: true }).type(text, { delay });
  }

  clearTableSearchInput() {
    this.waitDataTableToLoad();

    return cy.get('.cds--search-close').first().click({ force: true });
  }

  // Click the action button
  clickActionButton(action: string) {
    cy.get('[data-testid="table-action-btn"]').first().click({ force: true }); // open submenu
    cy.get(`button.${action}`).click({ force: true }); // click on "action" menu item
  }

  clickActionButtonFromMultiselect(content: string, action?: string) {
    this.searchTable(content);
    cy.wait(500);
    cy.contains('[cdstablerow] [cdstabledata]', content)
      .parent('[cdstablerow]')
      .find('[cdstablecheckbox] cds-checkbox [type="checkbox"]')
      .check({ force: true });
    if (action) {
      cy.get(`cds-table-toolbar-actions button.${action}`).click();
    }
  }

  /**
   * Clicks on the kebab menu button and performs an action on same row as content provided
   * @param content content to be found in a table cell
   * @param action action to be performed
   * @param waitTime default 1s. wait time between search resumes and start of looking up content
   * @param searchDelay delay time in ms between key strokes on search bar
   */
  clickRowActionButton(content: string, action: string, waitTime = 1 * 1000, searchDelay?: number) {
    this.waitDataTableToLoad();
    this.clearTableSearchInput();
    cy.contains('Creating...').should('not.exist');
    this.searchTable(content, searchDelay);
    cy.wait(waitTime);
    cy.contains('[cdstablerow] [cdstabledata]', content)
      .parent('[cdstablerow]')
      .find('[cdstabledata] [data-testid="table-action-btn"]')
      .click({ force: true });
    cy.wait(waitTime);
    cy.get(`button.${action}`).click({ force: true });
  }

  /**
   * This is a generic method to delete table rows.
   * It will select the first row that contains the provided name and delete it.
   * After that it will wait until the row is no longer displayed.
   * @param name The string to search in table cells.
   * @param columnIndex If provided, search string in columnIndex column.
   */
  // cdsModal is a temporary variable which will be removed once the carbonization
  // is complete
  delete(
    name: string,
    columnIndex?: number,
    section?: string,
    cdsModal = false,
    isMultiselect = false,
    shouldReload = false,
    confirmInput = false
  ) {
    const action: string = section === 'hosts' ? 'remove' : 'delete';

    // Clicks on table Delete/Remove button
    if (isMultiselect) {
      this.clickActionButtonFromMultiselect(name, action);
    } else {
      this.clickRowActionButton(name, action);
    }

    // Convert action to SentenceCase and Confirms deletion
    const actionUpperCase = action.charAt(0).toUpperCase() + action.slice(1);
    if (confirmInput) {
      cy.get('cds-modal input#resource_name').type(name, { force: true });
    } else {
      cy.get('[aria-label="confirmation"]').click({ force: true });
    }

    if (cdsModal) {
      cy.get('cds-modal button').contains(actionUpperCase).click({ force: true });
      // Wait for modal to close
      cy.get('cds-modal').should('not.exist');
    } else {
      cy.contains('cd-modal button', actionUpperCase).click();
      // Wait for modal to close
      cy.get('cd-modal').should('not.exist');
    }
    // Waits for item to be removed from table
    if (shouldReload) {
      cy.reload(true, { log: true, timeout: 5 * 1000 });
    }
    (columnIndex
      ? this.getTableCell(columnIndex, name, true)
      : this.getFirstTableCell(name)
    ).should('not.exist');
  }

  getNestedTableCell(
    selector: string,
    columnIndex: number,
    exactContent: string,
    partialMatch = false
  ) {
    this.waitDataTableToLoad();
    this.clearTableSearchInput();
    this.searchNestedTable(selector, exactContent);
    if (partialMatch) {
      return cy
        .get(`${selector} [cdstablerow] [cdstabledata]:nth-child(${columnIndex})`)
        .should('contain', exactContent);
    }
    return cy
      .get(`${selector}`)
      .contains(
        `[cdstablerow] [cdstabledata]:nth-child(${columnIndex})`,
        new RegExp(`^${exactContent}$`)
      );
  }

  searchNestedTable(selector: string, text: string) {
    this.waitDataTableToLoad();

    this.setPageSize('10');
    cy.get(`${selector} [aria-label=search]`).first().clear({ force: true }).type(text);
  }
}
