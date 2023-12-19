import { OSDsPageHelper } from '../cluster/osds.po';
import { DashboardPageHelper } from '../ui/dashboard.po';
import { ManagerModulesPageHelper } from '../cluster/mgr-modules.po';

describe('OSDs page', () => {
  const osds = new OSDsPageHelper();
  const dashboard = new DashboardPageHelper();
  const mgrmodules = new ManagerModulesPageHelper();

  before(() => {
    cy.login();
    mgrmodules.navigateTo();
    mgrmodules.navigateEdit('dashboard');
    cy.get('#FEATURE_TOGGLE_DASHBOARD').uncheck();
    cy.contains('button', 'Update').click();
  });

  beforeEach(() => {
    cy.login();
    osds.navigateTo();
  });

  describe('when Orchestrator is available', () => {
    it('should create and delete OSDs', () => {
      osds.getTableCount('total').as('initOSDCount');
      osds.navigateTo('create');
      osds.create('hdd');

      cy.get('@newOSDCount').then((newCount) => {
        cy.get('@initOSDCount').then((oldCount) => {
          const expectedCount = Number(oldCount) + Number(newCount);

          // check total rows
          osds.expectTableCount('total', expectedCount);

          // landing page is easier to check OSD status
          dashboard.navigateTo();
          dashboard.infoCardBody('OSDs').should('contain.text', `${expectedCount} total`);
          dashboard.infoCardBody('OSDs').should('contain.text', `${expectedCount} up`);
          dashboard.infoCardBody('OSDs').should('contain.text', `${expectedCount} in`);

          cy.wait(30000);
          expect(Number(newCount)).to.be.gte(2);
          // Delete the first OSD we created
          osds.navigateTo();
          const deleteOsdId = Number(oldCount);
          osds.deleteByIDs([deleteOsdId], false);
          osds.ensureNoOsd(deleteOsdId);

          cy.wait(30000);
          // Replace the second OSD we created
          const replaceID = Number(oldCount) + 1;
          osds.deleteByIDs([replaceID], true);
          osds.checkStatus(replaceID, ['destroyed']);
        });
      });
    });
  });
});
