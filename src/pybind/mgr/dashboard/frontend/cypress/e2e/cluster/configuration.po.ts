import { PageHelper } from '../page-helper.po';

export class ConfigurationPageHelper extends PageHelper {
  pages = {
    index: { url: '#/configuration', id: 'cd-configuration' }
  };

  /**
   * Clears out all the values in a config to reset before and after testing
   * Does not work for configs with checkbox only, possible future PR
   */
  configClear(name: string) {
    const valList = ['global', 'mon', 'mgr', 'osd', 'mds', 'client']; // Editable values

    this.navigateEdit(name);
    // Waits for the data to load
    cy.contains('.card-header', `Edit ${name}`);

    for (const i of valList) {
      cy.get(`#${i}`).clear();
    }
    // Clicks save button and checks that values are not present for the selected config
    cy.get('[data-cy=submitBtn]').click();

    // Enter config setting name into filter box
    this.searchTable(name);

    // Expand row
    this.getExpandCollapseElement(name).click();

    // Checks for visibility of details tab
    this.getStatusTables().should('be.visible');

    for (const i of valList) {
      // Waits until values are not present in the details table
      this.getStatusTables().should('not.contain.text', i + ':');
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
    this.navigateEdit(name);

    // Waits for data to load
    cy.contains('.card-header', `Edit ${name}`);

    values.forEach((valtuple) => {
      // Finds desired value based off given list
      cy.get(`#${valtuple[0]}`).type(valtuple[1]); // of values and inserts the given number for the value
    });

    // Clicks save button then waits until the desired config is visible, clicks it,
    // then checks that each desired value appears with the desired number
    cy.get('[data-cy=submitBtn]').click();

    // Enter config setting name into filter box
    this.searchTable(name);

    // Checks for visibility of config in table
    this.getExpandCollapseElement(name).should('be.visible').click();

    // Clicks config
    values.forEach((value) => {
      // iterates through list of values and
      // checks if the value appears in details with the correct number attatched
      cy.contains('.table.table-striped.table-bordered', `${value[0]}\: ${value[1]}`);
    });
  }
}
