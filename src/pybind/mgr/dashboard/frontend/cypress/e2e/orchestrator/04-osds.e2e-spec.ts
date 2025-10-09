import { OSDsPageHelper } from '../cluster/osds.po';
import { DashboardV3PageHelper } from '../ui/dashboard-v3.po';

describe('OSDs page', () => {
  const osds = new OSDsPageHelper();
  const dashboard = new DashboardV3PageHelper();

  before(() => {
    cy.login();
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
          dashboard.cardRow('OSD').should('contain.text', `${expectedCount} OSDs`);

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
