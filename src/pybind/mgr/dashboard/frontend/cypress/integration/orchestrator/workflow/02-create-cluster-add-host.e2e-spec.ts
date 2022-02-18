import {
  CreateClusterHostPageHelper,
  CreateClusterWizardHelper
} from 'cypress/integration/cluster/create-cluster.po';

describe('Create cluster add host page', () => {
  const createCluster = new CreateClusterWizardHelper();
  const createClusterHostPage = new CreateClusterHostPageHelper();
  const hostnames = ['ceph-node-00', 'ceph-node-01', 'ceph-node-02', 'ceph-node-[01-03]'];
  const addHost = (hostname: string, exist?: boolean, pattern?: boolean, labels: string[] = []) => {
    cy.get('button[data-testid=table-action-button]').click();
    createClusterHostPage.add(hostname, exist, false, labels);
    if (!pattern) {
      createClusterHostPage.checkExist(hostname, true);
    }
  };

  beforeEach(() => {
    cy.login();
    Cypress.Cookies.preserveOnce('token');
    createCluster.navigateTo();
    createCluster.createCluster();
  });

  it('should check if title contains Add Hosts', () => {
    cy.get('.nav-link').should('contain.text', 'Add Hosts');

    cy.get('.title').should('contain.text', 'Add Hosts');
  });

  it('should check existing host and add new hosts', () => {
    createClusterHostPage.checkExist(hostnames[0], true);

    addHost(hostnames[1], false);
    addHost(hostnames[2], false);
    createClusterHostPage.remove(hostnames[1]);
    createClusterHostPage.remove(hostnames[2]);
    addHost(hostnames[3], false, true);
  });

  it('should remove a host', () => {
    createClusterHostPage.remove(hostnames[1]);
  });

  it('should add a host with some predefined labels and verify it', () => {
    const labels = ['mon', 'mgr', 'rgw', 'osd'];
    addHost(hostnames[1], false, false, labels);
    createClusterHostPage.checkLabelExists(hostnames[1], labels, true);
  });

  it('should verify "_no_schedule" label is added', () => {
    createClusterHostPage.checkLabelExists(hostnames[1], ['_no_schedule'], true);
    createClusterHostPage.checkLabelExists(hostnames[2], ['_no_schedule'], true);
  });

  it('should not add an existing host', () => {
    cy.get('.btn.btn-accent').first().click({ force: true });
    createClusterHostPage.add(hostnames[0], true);
  });

  it('should edit host labels', () => {
    const labels = ['foo', 'bar'];
    createClusterHostPage.editLabels(hostnames[0], labels, true);
    createClusterHostPage.editLabels(hostnames[0], labels, false);
  });
});
