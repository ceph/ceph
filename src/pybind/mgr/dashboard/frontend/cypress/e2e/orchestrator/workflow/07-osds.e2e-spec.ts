/* tslint:disable*/
import { OSDsPageHelper } from '../../cluster/osds.po';
/* tslint:enable*/

describe('OSDs page', () => {
  const osds = new OSDsPageHelper();

  beforeEach(() => {
    cy.login();
    osds.navigateTo();
  });

  describe('Create OSDs tearsheet', () => {
    // Runs in cephadm e2e where Orchestrator is available.
    it('should open create modal and close it on cancel, updating the URL', () => {
      osds.openCreateTearsheet();
      cy.get('[data-testid="osd-create-tearsheet-header"]').should('contain.text', 'Create OSDs');
      cy.location('hash').should('eq', '#/osd/(modal:create)');

      // Cancel closes the modal and redirects back to /osd
      osds.closeCreateTearsheet();
    });
  });

  it('should check if atleast 3 osds are created', { retries: 3 }, () => {
    // we have created a total of more than 3 osds throughout
    // the whole tests so ensuring that atleast
    // 3 osds are listed in the table. Since the OSD
    // creation can take more time going with
    // retry of 3
    for (let id = 0; id < 3; id++) {
      osds.checkStatus(id, ['in', 'up']);
    }
  });
});
