import { ConfigurationPageHelper } from './configuration.po';

describe('RGW configuration page', () => {
  const configurations = new ConfigurationPageHelper();

  beforeEach(() => {
    cy.login();
    configurations.navigateTo();
  });

  describe('breadcrumb and tab tests', () => {
    it('should open and show breadcrumb', () => {
      configurations.expectBreadcrumbText('Configuration');
    });

    it('should show one tab', () => {
      configurations.getTabsCount().should('eq', 1);
    });

    it('should show Server-side Encryption Config list tab at first', () => {
      configurations.getTabText(0).should('eq', 'Server-side Encryption');
    });
  });

  describe('create and edit encryption configuration', () => {
    it('should create configuration', () => {
      configurations.create('vault', 'agent', 'transit', 'https://localhost:8080');
      configurations.getFirstTableCell('kms').should('exist');
    });

    it('should edit configuration', () => {
      configurations.edit('https://localhost:9090');
      configurations.getDataTables().should('contain.text', 'https://localhost:9090');
    });
  });

  describe('check bucket encryption checkbox', () => {
    it('should ensure encryption checkbox to be enabled in bucket form', () => {
      configurations.checkBucketEncryption();
    });
  });
});
