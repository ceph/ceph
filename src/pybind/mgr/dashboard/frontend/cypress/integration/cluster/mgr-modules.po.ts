import { PageHelper } from '../page-helper.po';

export class ManagerModulesPageHelper extends PageHelper {
  pages = { index: { url: '#/mgr-modules', id: 'cd-mgr-module-list' } };

  /**
   * Selects the Manager Module and then fills in the desired fields.
   * Doesn't check/uncheck boxes because it is not reflected in the details table.
   * DOES NOT WORK FOR ALL MGR MODULES, for example, Device health
   */
  editMgrModule(name: string, tuple: string[][]) {
    this.navigateEdit(name);

    for (const entry of tuple) {
      // Clears fields and adds edits
      cy.get(`#${entry[1]}`).clear().type(entry[0]);
    }

    cy.contains('button', 'Update').click();
    // Checks if edits appear
    this.getExpandCollapseElement(name).should('be.visible').click();
    for (const entry of tuple) {
      cy.get('.datatable-body').last().contains(entry[0]);
    }

    // Clear mgr module of all edits made to it
    this.navigateEdit(name);

    // Clears the editable fields
    for (const entry of tuple) {
      cy.get(`#${entry[1]}`).clear();
    }

    // Checks that clearing represents in details tab of module
    cy.contains('button', 'Update').click();
    this.getExpandCollapseElement(name).should('be.visible').click();
    for (const entry of tuple) {
      cy.get('.datatable-body').eq(1).should('contain', entry[1]).and('not.contain', entry[0]);
    }
  }

  /**
   * Selects the Devicehealth manager module, then fills in the desired fields,
   * including all fields except checkboxes.
   * Then checks if these edits appear in the details table.
   */
  editDevicehealth(
    threshhold?: string,
    pooln?: string,
    retention?: string,
    scrape?: string,
    sleep?: string,
    warn?: string
  ) {
    let devHealthArray: [string, string][];
    devHealthArray = [
      [threshhold, 'mark_out_threshold'],
      [pooln, 'pool_name'],
      [retention, 'retention_period'],
      [scrape, 'scrape_frequency'],
      [sleep, 'sleep_interval'],
      [warn, 'warn_threshold']
    ];

    this.navigateEdit('devicehealth');
    for (let i = 0, devHealthTuple; (devHealthTuple = devHealthArray[i]); i++) {
      if (devHealthTuple[0] !== undefined) {
        // Clears and inputs edits
        cy.get(`#${devHealthTuple[1]}`).type(devHealthTuple[0]);
      }
    }

    cy.contains('button', 'Update').click();
    this.getFirstTableCell('devicehealth').should('be.visible');
    // Checks for visibility of devicehealth in table
    this.getExpandCollapseElement('devicehealth').click();
    for (let i = 0, devHealthTuple: [string, string]; (devHealthTuple = devHealthArray[i]); i++) {
      if (devHealthTuple[0] !== undefined) {
        // Repeatedly reclicks the module to check if edits has been done
        cy.contains('.datatable-body-cell-label', 'devicehealth').click();
        cy.get('.datatable-body').last().contains(devHealthTuple[0]).should('be.visible');
      }
    }

    // Inputs old values into devicehealth fields. This manager module doesn't allow for updates
    // to be made when the values are cleared. Therefore, I restored them to their original values
    // (on my local run of ceph-dev, this is subject to change i would assume).
    // I'd imagine there is a better way of doing this.
    this.navigateEdit('devicehealth');

    cy.get('#mark_out_threshold').clear().type('2419200');

    cy.get('#pool_name').clear().type('device_health_metrics');

    cy.get('#retention_period').clear().type('15552000');

    cy.get('#scrape_frequency').clear().type('86400');

    cy.get('#sleep_interval').clear().type('600');

    cy.get('#warn_threshold').clear().type('7257600');

    // Checks that clearing represents in details tab
    cy.contains('button', 'Update').click();
    this.getExpandCollapseElement('devicehealth').should('be.visible').click();
    for (let i = 0, devHealthTuple: [string, string]; (devHealthTuple = devHealthArray[i]); i++) {
      if (devHealthTuple[0] !== undefined) {
        // Repeatedly reclicks the module to check if clearing has been done
        cy.contains('.datatable-body-cell-label', 'devicehealth').click();
        cy.get('.datatable-body')
          .eq(1)
          .should('contain', devHealthTuple[1])
          .and('not.contain', devHealthTuple[0]);
      }
    }
  }
}
