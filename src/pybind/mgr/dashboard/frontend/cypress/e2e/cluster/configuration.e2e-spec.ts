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

  describe('resource page overview check', () => {
    it('should open the first resource page and show the overview card', () => {
      configuration.getResourcePage().should('be.visible').click();
      configuration.getOverviewField('Name').should('be.visible');
      configuration.getOverviewField('Current values').should('be.visible');
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

    it('should click and edit a configuration and results should appear in the overview card', () => {
      configuration.edit(
        configName,
        ['global', '1'],
        ['mon', '2'],
        ['mgr', '3'],
        ['osd', '4'],
        ['mds', '5']
      );
    });

    it('should verify modified filter is applied properly', () => {
      configuration.clearFilter();
      configuration.getTableCount('found').as('configFound');
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
