import { OSDsPageHelper } from './osds.po';

describe('OSDs page', () => {
  const osds = new OSDsPageHelper();

  beforeEach(() => {
    cy.login();
    osds.navigateTo();
  });

  describe('breadcrumb and tab tests', () => {
    it('should open and show breadcrumb', () => {
      osds.expectBreadcrumbText('OSDs');
    });

    it('should show two tabs', () => {
      osds.getTabsCount().should('eq', 2);
      osds.getTabText(0).should('eq', 'OSDs List');
      osds.getTabText(1).should('eq', 'Overall Performance');
    });
  });

  describe('check existence of fields on OSD page', () => {
    it('should check that number of rows and count in footer match', () => {
      osds.getTableCount('total').then((text) => {
        osds.getTableRows().its('length').should('equal', text);
      });
    });

    it('should verify that buttons exist', () => {
      cy.contains('button', 'Create');
      cy.contains('button', 'Cluster-wide configuration');
    });

    describe('by selecting one row in OSDs List', () => {
      beforeEach(() => {
        osds.getExpandCollapseElement().click();
      });

      it('should show the correct text for the tab labels', () => {
        cy.get('#tabset-osd-details > a').then(($tabs) => {
          const tabHeadings = $tabs.map((_i, e) => e.textContent).get();

          expect(tabHeadings).to.eql([
            'Devices',
            'Attributes (OSD map)',
            'Metadata',
            'Device health',
            'Performance counter',
            'Performance Details'
          ]);
        });
      });
    });
  });

  describe('Create OSDs button', () => {
    // Dashboard e2e uses test_orchestrator (available) with dummy inventory.
    // Force an empty inventory so Create is disabled for the no-devices case.
    it('should be disabled when no eligible devices are available', () => {
      cy.intercept('GET', /\/ui-api\/host\/inventory/, { body: [] }).as('hostInventory');
      // Leave /osd first so returning remounts the component and re-fetches inventory
      // under the intercept (same-route visit alone may not re-run ngOnInit).
      cy.visit('#/overview');
      cy.get('cd-overview').should('exist');
      osds.navigateTo();
      cy.wait('@hostInventory');
      osds.getCreateButton().should('be.disabled');
    });
  });
});
