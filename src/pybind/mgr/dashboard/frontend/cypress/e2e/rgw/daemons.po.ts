import { PageHelper } from '../page-helper.po';

export class DaemonsPageHelper extends PageHelper {
  pages = {
    index: { url: '#/rgw/daemon', id: 'cd-rgw-daemon-list' }
  };

  getTableCell() {
    return cy
      .get('.tab-content')
      .its(1)
      .find('cd-table')
      .should('have.length', 1) // Only 1 table should be renderer
      .find('datatable-body-cell');
  }

  checkTables() {
    // click on a daemon so details table appears
    cy.get('.datatable-body-cell-label').first().click();

    // check details table is visible
    // check at least one field is present
    this.getTableCell().should('be.visible').should('contain.text', 'ceph_version');

    // click on performance counters tab and check table is loaded
    cy.contains('.nav-link', 'Performance Counters').click();

    // check at least one field is present
    this.getTableCell().should('be.visible').should('contain.text', 'objecter.op_r');

    // click on performance details tab
    cy.contains('.nav-link', 'Performance Details').click();
  }
}
