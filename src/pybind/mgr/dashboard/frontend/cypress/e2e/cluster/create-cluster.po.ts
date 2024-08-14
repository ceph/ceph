import { PageHelper } from '../page-helper.po';
import { NotificationSidebarPageHelper } from '../ui/notification.po';
import { HostsPageHelper } from './hosts.po';
import { ServicesPageHelper } from './services.po';

const pages = {
  index: { url: '#/expand-cluster?welcome=true', id: 'cd-create-cluster' }
};
export class CreateClusterWizardHelper extends PageHelper {
  pages = pages;

  createCluster() {
    cy.get('cd-create-cluster').should('contain.text', 'Please expand your cluster first');
    cy.get('[name=expand-cluster]').click();
    cy.get('cd-wizard').should('exist');
  }

  doSkip() {
    cy.get('[name=skip-cluster-creation]').click();
    cy.contains('cd-modal button', 'Continue').click();

    cy.get('cd-dashboard').should('exist');
    const notification = new NotificationSidebarPageHelper();
    notification.open();
    notification.getNotifications().should('contain', 'Cluster expansion skipped by user');
  }
}

export class CreateClusterHostPageHelper extends HostsPageHelper {
  pages = {
    index: { url: '#/expand-cluster?welcome=true', id: 'cd-wizard' },
    add: { url: '', id: 'cd-host-form' }
  };

  columnIndex = {
    hostname: 1,
    labels: 2,
    status: 3,
    services: 0
  };
}

export class CreateClusterServicePageHelper extends ServicesPageHelper {
  pages = {
    index: { url: '#/expand-cluster?welcome=true', id: 'cd-wizard' },
    create: { url: '', id: 'cd-service-form' }
  };

  columnIndex = {
    service_name: 1,
    placement: 2,
    running: 0,
    size: 0,
    last_refresh: 0
  };
}
