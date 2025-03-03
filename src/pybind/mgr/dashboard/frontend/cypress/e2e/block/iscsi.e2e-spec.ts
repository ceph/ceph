import { IscsiPageHelper } from './iscsi.po';

describe('Iscsi Page', () => {
  const iscsi = new IscsiPageHelper();

  beforeEach(() => {
    cy.login();
    iscsi.navigateTo();
  });

  it('should open and show breadcrumb', () => {
    iscsi.expectBreadcrumbText('Overview');
  });

  it('should check that tables are displayed and legends are correct', () => {
    // Check tables are displayed
    iscsi.getDataTables().its(0).should('be.visible');
    iscsi.getDataTables().its(1).should('be.visible');

    // Check that legends are correct
    iscsi.getLegends().its(0).should('contain.text', 'Gateways');
    iscsi.getLegends().its(1).should('contain.text', 'Images');
  });
});
