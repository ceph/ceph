import { ConfigurationPageHelper } from './configuration.po';

describe('Configuration page', () => {
  const configuration = new ConfigurationPageHelper();

  beforeEach(() => {
    cy.login();
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

    it('should verify that selected footer increases when an entry is clicked', () => {
      configuration.getTableSelectedCount().should('eq', 1);
    });

    it('should check that details table opens and tab is correct', () => {
      configuration.getStatusTables().should('be.visible');
      configuration.getTabsCount().should('eq', 1);
      configuration.getTabText(0).should('eq', 'Details');
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
      configuration.getTableFoundCount().should('eq', 1);
    });

    it('should hide all modified configurations', () => {
      configuration.filterTable('Modified', 'no');
      configuration.getTableFoundCount().should('gt', 1);
    });
  });
});
