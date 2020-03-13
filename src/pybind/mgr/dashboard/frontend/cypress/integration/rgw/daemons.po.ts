import { PageHelper } from '../page-helper.po';

export class DaemonsPageHelper extends PageHelper {
  pages = {
    index: { url: '#/rgw/daemon', id: 'cd-rgw-daemon-list' }
  };

  getTableCell(tableIndex: number) {
    return cy
      .get('.tab-container')
      .its(1)
      .find('cd-table')
      .its(tableIndex)
      .find('datatable-body-cell');
  }

  checkTables() {
    // click on a daemon so details table appears
    cy.get('.datatable-body-cell-label').first().click();

    // check details table is visible
    // check at least one field is present
    this.getTableCell(0).should('visible').should('contain.text', 'ceph_version');
    // check performance counters table is not currently visible
    this.getTableCell(1).should('not.be.visible');

    // click on performance counters tab and check table is loaded
    cy.contains('.nav-link', 'Performance Counters').click();

    // check at least one field is present
    this.getTableCell(1).should('be.visible').should('contain.text', 'objecter.op_r');
    // check details table is not currently visible
    this.getTableCell(0).should('not.be.visible');

    // click on performance details tab
    cy.contains('.nav-link', 'Performance Details').click();

    // checks the other tabs' content isn't visible
    this.getTableCell(0).should('not.be.visible');
    this.getTableCell(1).should('not.be.visible');
  }
}
