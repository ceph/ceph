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

    it('should check that details table opens (w/o tab header)', () => {
      configuration.getStatusTables().should('be.visible');
      configuration.getTabs().should('not.exist');
    });
  });

  describe('edit configuration test', () => {
    const configName = 'client_cache_size';

    beforeEach(() => {
      configuration.clearTableSearchInput();
      configuration.getTableCount('found').as('configFound');
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

    it('should verify modified filter is applied properly', () => {
      configuration.filterTable('Modified', 'no');
      configuration.getTableCount('found').as('unmodifiedConfigs');

      // Modified filter value to yes
      configuration.filterTable('Modified', 'yes');
      configuration.getTableCount('found').as('modifiedConfigs');

      cy.get('@configFound').then((configFound) => {
        cy.get('@unmodifiedConfigs').then((unmodifiedConfigs) => {
          const modifiedConfigs = Number(configFound) - Number(unmodifiedConfigs);
          configuration.getTableCount('found').should('eq', modifiedConfigs);
        });
      });

      // Modified filter value to no
      configuration.filterTable('Modified', 'no');
      cy.get('@configFound').then((configFound) => {
        cy.get('@modifiedConfigs').then((modifiedConfigs) => {
          const unmodifiedConfigs = Number(configFound) - Number(modifiedConfigs);
          configuration.getTableCount('found').should('eq', unmodifiedConfigs);
        });
      });
    });
  });
});
