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
      // check for table header 'Status'
      monitors.getLegends().its(0).should('have.text', 'Status');

      // check for fields in table
      monitors
        .getStatusTables()
        .should('contain.text', 'Cluster ID')
        .and('contain.text', 'monmap modified')
        .and('contain.text', 'monmap epoch')
        .and('contain.text', 'quorum con')
        .and('contain.text', 'quorum mon')
        .and('contain.text', 'required con')
        .and('contain.text', 'required mon');
    });

    it('should check In Quorum and Not In Quorum tables are present', () => {
      // check for there to be two tables
      monitors.getDataTables().should('have.length', 2);

      // check for table header 'In Quorum'
      monitors.getLegends().its(1).should('have.text', 'In Quorum');

      // check for table header 'Not In Quorum'
      monitors.getLegends().its(2).should('have.text', 'Not In Quorum');

      // verify correct columns on In Quorum table
      monitors.getDataTableHeaders(0).contains('Name');

      monitors.getDataTableHeaders(0).contains('Rank');

      monitors.getDataTableHeaders(0).contains('Public Address');

      monitors.getDataTableHeaders(0).contains('Open Sessions');

      // verify correct columns on Not In Quorum table
      monitors.getDataTableHeaders(1).contains('Name');

      monitors.getDataTableHeaders(1).contains('Rank');

      monitors.getDataTableHeaders(1).contains('Public Address');
    });
  });
});
