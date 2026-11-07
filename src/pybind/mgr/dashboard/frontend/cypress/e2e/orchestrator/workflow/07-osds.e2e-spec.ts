/* tslint:disable*/
import { OSDsPageHelper } from '../../cluster/osds.po';
/* tslint:enable*/

describe('OSDs page', () => {
  const osds = new OSDsPageHelper();

  beforeEach(() => {
    cy.login();
    osds.navigateTo();
    // OSD creation from prior workflow specs can leave the UI busy; wait for
    // the list to mount before assertions run.
    cy.get('cd-osd-list', { timeout: 30000 }).should('be.visible');
    cy.get('cd-osd-list cds-table', { timeout: 30000 }).should('exist');
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
