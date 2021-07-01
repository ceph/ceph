import { PageHelper } from '../page-helper.po';
import { NotificationSidebarPageHelper } from '../ui/notification.po';

export class CreateClusterWelcomePageHelper extends PageHelper {
  pages = {
    index: { url: '#/create-cluster', id: 'cd-create-cluster' }
  };

  createCluster() {
    cy.get('cd-create-cluster').should('contain.text', 'Welcome to Ceph');
    cy.get('[name=create-cluster]').click();
  }

  doSkip() {
    cy.get('[name=skip-cluster-creation]').click();

    cy.get('cd-dashboard').should('exist');
    const notification = new NotificationSidebarPageHelper();
    notification.open();
    notification.getNotifications().should('contain', 'Cluster creation skipped by user');
  }
}
