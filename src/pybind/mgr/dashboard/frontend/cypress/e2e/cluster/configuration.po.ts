import { PageHelper } from '../page-helper.po';

export class ConfigurationPageHelper extends PageHelper {
  pages = {
    index: { url: '#/configuration', id: 'cd-configuration' }
  };

  getOverviewField(label: string) {
    return cy
      .get('.cd-overview-label')
      .filter((_index, el) => el.textContent?.includes(label))
      .closest('.cd-overview-item');
  }

  private waitForEditForm(name: string) {
    cy.contains('h3', `Edit ${name}`).should('be.visible');
  }

  private getSectionInput(section: string) {
    return cy.get(`input#${section}`);
  }

  /**
   * Clears out all the values in a config to reset before and after testing
   * Does not work for configs with checkbox only, possible future PR
   */
  configClear(name: string) {
    this.navigateTo();
    const valList = ['global', 'mon', 'mgr', 'osd', 'mds']; // Editable values (client moved to separate section)
    this.searchTable(name, 100);
    this.getTableRow(name).find('[cdstabledata]').eq(1).click();
    cy.contains('button', 'Edit').click();
    this.waitForEditForm(name);

    for (const i of valList) {
      this.getSectionInput(i).clear({ force: true }).blur({ force: true });
    }
    // Clicks save button and checks that values are not present for the selected config
    cy.get('[data-testid=submitBtn]').click();

    cy.url().should('include', '#/configuration');
    cy.get(this.pages.index.id);

    this.clearFilter();

    // Enter config setting name into filter box
    this.searchTable(name, 100);

    // Open the resource page for the config and verify the overview card values are cleared.
    this.getResourcePage(name).should('be.visible').click();

    for (const i of valList) {
      this.getOverviewField('Current values').should('not.contain.text', `${i}:`);
    }
  }

  /**
   * Clicks the designated config, then inputs the values passed into the edit function.
   * Then checks if the edit is reflected in the config table.
   * Takes in name of config and a list of tuples of values the user wants edited,
   * each tuple having the desired value along with the number tehey want for that value.
   * Ex: [global, '2'] is the global value with an input of 2
   */
  edit(name: string, ...values: [string, string][]) {
    this.clearFilter();
    this.searchTable(name, 100);
    this.getTableRow(name).find('[cdstabledata]').eq(1).click();
    cy.contains('button', 'Edit').click();

    this.waitForEditForm(name);

    values.forEach((valtuple) => {
      // Finds desired value based off given list
      this.getSectionInput(valtuple[0]).type(valtuple[1]);
    });

    // Clicks save button then waits until the desired config is visible, clicks it,
    // then checks that each desired value appears with the desired number
    cy.get('[data-testid=submitBtn]').click();
    cy.url().should('include', '#/configuration');
    cy.get(this.pages.index.id);

    // Enter config setting name into filter box
    this.searchTable(name, 100);

    // Open the resource page for the config and verify the overview card values.
    this.getResourcePage(name).should('be.visible').click();

    values.forEach((value) => {
      this.getOverviewField('Current values').should('contain.text', `${value[0]}: ${value[1]}`);
    });
  }

  clearFilter() {
    cy.get('div.filter-tags') // Find the div with class filter-tags
      .find('button.cds--btn.cds--btn--ghost') // Find the button with specific classes
      .contains('Clear filters') // Ensure the button contains the text "Clear filters"
      .should('be.visible') // Assert that the button is visible
      .click();
  }
}
