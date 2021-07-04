import { CreateClusterWizardHelper } from 'cypress/integration/cluster/create-cluster.po';

describe('Create cluster add host page', () => {
  const createCluster = new CreateClusterWizardHelper();
  const hostnames = [
    'ceph-node-00.cephlab.com',
    'ceph-node-01.cephlab.com',
    'ceph-node-02.cephlab.com'
  ];
  const addHost = (hostname: string, exist?: boolean) => {
    createCluster.add(hostname, exist, true);
    createCluster.checkExist(hostname, true);
  };

  beforeEach(() => {
    cy.login();
    Cypress.Cookies.preserveOnce('token');
    createCluster.navigateTo();
    createCluster.createCluster();
  });

  it('should check if nav-link and title contains Add Hosts', () => {
    cy.get('.nav-link').should('contain.text', 'Add Hosts');

    cy.get('.title').should('contain.text', 'Add Hosts');
  });

  it('should check existing host and add new hosts into maintenance mode', () => {
    createCluster.checkExist(hostnames[0], true);

    addHost(hostnames[1], false);
    addHost(hostnames[2], false);
  });

  it('should not add an existing host', () => {
    createCluster.add(hostnames[0], true);
  });

  it('should edit host labels', () => {
    const labels = ['foo', 'bar'];
    createCluster.editLabels(hostnames[0], labels, true);
    createCluster.editLabels(hostnames[0], labels, false);
  });

  it('should delete a host', () => {
    createCluster.delete(hostnames[1]);
  });
});
