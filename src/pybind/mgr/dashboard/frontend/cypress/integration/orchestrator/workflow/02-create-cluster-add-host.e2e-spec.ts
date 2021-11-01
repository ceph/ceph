import {
  CreateClusterHostPageHelper,
  CreateClusterWizardHelper
} from 'cypress/integration/cluster/create-cluster.po';

describe('Create cluster add host page', () => {
  const createCluster = new CreateClusterWizardHelper();
  const createClusterHostPage = new CreateClusterHostPageHelper();
  const hostnames = [
    'ceph-node-00.cephlab.com',
    'ceph-node-01.cephlab.com',
    'ceph-node-02.cephlab.com',
    'ceph-node-[01-02].cephlab.com'
  ];
  const addHost = (hostname: string, exist?: boolean, pattern?: boolean) => {
    cy.get('.btn.btn-accent').first().click({ force: true });
    createClusterHostPage.add(hostname, exist, false);
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
    createClusterHostPage.delete(hostnames[1]);
    createClusterHostPage.delete(hostnames[2]);
    addHost(hostnames[3], false, true);
  });

  it('should delete a host and add it back', () => {
    createClusterHostPage.delete(hostnames[1]);
    addHost(hostnames[1], false);
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
