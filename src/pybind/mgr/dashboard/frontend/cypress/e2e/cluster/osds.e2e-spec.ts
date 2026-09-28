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

  describe('Create OSDs tearsheet', () => {
    it('should open create modal and close it on cancel, updating the URL', function () {
      // Only exercise the Create click flow when the button is enabled.
      // When Create is disabled (no Orchestrator / eligible devices), opening the
      // form via URL is not the user path and orch APIs can return 500s.
      cy.get('#osd-actions [data-testid="primary-action"][aria-label="Create"]').then(
        function ($btn) {
          if ($btn.is(':disabled')) {
            cy.log('Skipping: Create OSD is disabled in this environment');
            this.skip();
          }

          cy.wrap($btn).click();
          cy.get('cd-osd-form').should('exist');
          cy.get('[data-testid="osd-create-tearsheet-header"]').should(
            'contain.text',
            'Create OSDs'
          );
          cy.location('hash').should('eq', '#/osd/(modal:create)');

          // Cancel closes the modal and redirects back to /osd
          osds.closeCreateTearsheet();
        }
      );
    });
  });
});
