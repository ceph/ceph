import { PageHelper } from '../page-helper.po';

export class MonitorsPageHelper extends PageHelper {
  pages = {
    index: { url: '#/monitor', id: 'cd-monitor' }
  };

  getStatusTable() {
    cy.get('cd-monitor cd-table table[cdstable] tbody').should('exist');
    cy.contains('Loading').should('not.exist');
    return cy.get('cd-monitor fieldset table');
  }

  getMonitorTable() {
    return cy.get('cd-monitor cd-table');
  }

  getMonitorTableHeaders() {
    return this.getMonitorTable().find('th');
  }
}
