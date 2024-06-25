import { MultisitePageHelper } from './multisite.po';

describe('Multisite page', () => {
  const multisite = new MultisitePageHelper();

  beforeEach(() => {
    cy.login();
    multisite.navigateTo();
  });

  describe('tabs and table tests', () => {
    it('should show two tabs', () => {
      multisite.getTabsCount().should('eq', 2);
    });

    it('should show Configuration tab as a first tab', () => {
      multisite.getTabText(0).should('eq', 'Configuration');
    });

    it('should show sync policy tab as a second tab', () => {
      multisite.getTabText(1).should('eq', 'Sync Policy');
    });

    it('should show empty table in Sync Policy page', () => {
      multisite.getTab('Sync Policy').click();
      multisite.getDataTables().should('exist');
      multisite.getTableCount('total').should('eq', 0);
    });
  });
});
