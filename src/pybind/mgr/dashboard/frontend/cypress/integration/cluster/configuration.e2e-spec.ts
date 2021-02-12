import { ConfigurationPageHelper } from './configuration.po';

describe('Configuration page', () => {
  const configuration = new ConfigurationPageHelper();

  beforeEach(() => {
    cy.login();
    Cypress.Cookies.preserveOnce('token');
    configuration.navigateTo();
  });

  describe('breadcrumb test', () => {
    it('should open and show breadcrumb', () => {
      configuration.expectBreadcrumbText('Configuration');
    });
  });

  describe('fields check', () => {
    beforeEach(() => {
      configuration.getExpandCollapseElement().click();
    });

    it('should check that details table opens (w/o tab header)', () => {
      configuration.getStatusTables().should('be.visible');
      configuration.getTabs().should('not.exist');
    });
  });

  describe('edit configuration test', () => {
    const configName = 'client_cache_size';

    beforeEach(() => {
      configuration.clearTableSearchInput();
    });

    after(() => {
      configuration.configClear(configName);
    });

    it('should click and edit a configuration and results should appear in the table', () => {
      configuration.edit(
        configName,
        ['global', '1'],
        ['mon', '2'],
        ['mgr', '3'],
        ['osd', '4'],
        ['mds', '5'],
        ['client', '6']
      );
    });

    it('should show only modified configurations', () => {
      configuration.filterTable('Modified', 'yes');
      configuration.getTableCount('found').should('eq', 2);
    });

    it('should hide all modified configurations', () => {
      configuration.filterTable('Modified', 'no');
      configuration.getTableCount('found').should('gt', 1);
    });
  });
});
