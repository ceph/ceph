import { MonitorsPageHelper } from './monitors.po';

describe('Monitors page', () => {
  const monitors = new MonitorsPageHelper();

  beforeEach(() => {
    cy.login();
    monitors.navigateTo();
  });

  describe('breadcrumb test', () => {
    it('should open and show breadcrumb', () => {
      monitors.expectBreadcrumbText('Monitors');
    });
  });

  describe('fields check', () => {
    it('should check status table is present', () => {
      monitors
        .getStatusTable()
        .should('be.visible')
        .and('contain.text', 'Cluster ID')
        .and('contain.text', 'monmap modified')
        .and('contain.text', 'monmap epoch')
        .and('contain.text', 'quorum con')
        .and('contain.text', 'quorum mon')
        .and('contain.text', 'required con')
        .and('contain.text', 'required mon');
    });

    it('should check monitors table is present', () => {
      monitors.getMonitorTable().should('be.visible');
      monitors.getDataTables().should('have.length', 1);

      monitors.getMonitorTable().find('h4').should('contain.text', 'Monitors');
      monitors
        .getMonitorTable()
        .find('p')
        .should('contain.text', 'Maintains the master copy of the cluster state');

      monitors.getMonitorTableHeaders().should('contain.text', 'Name');
      monitors.getMonitorTableHeaders().should('contain.text', 'Rank');
      monitors.getMonitorTableHeaders().should('contain.text', 'Public address');
      monitors.getMonitorTableHeaders().should('contain.text', 'In Quorum');
      monitors.getMonitorTableHeaders().should('contain.text', 'Open sessions');
    });
  });
});
