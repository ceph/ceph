import { PoolPageHelper } from '../pools/pools.po';
import { NotificationSidebarPageHelper } from './notification.po';

describe('Notification page', () => {
  const notification = new NotificationSidebarPageHelper();
  const pools = new PoolPageHelper();
  const poolName = 'e2e_notification_pool';

  before(() => {
    cy.login();
    pools.navigateTo('create');
    pools.create(poolName, 8);
    pools.edit_pool_pg(poolName, 4, false);
  });

  after(() => {
    cy.login();
    pools.navigateTo();
    pools.delete(poolName);
  });

  beforeEach(() => {
    cy.login();
    pools.navigateTo();
  });

  it('should open notification sidebar', () => {
    notification.getSidebar().should('not.be.visible');
    notification.open();
    notification.getSidebar().should('be.visible');
  });

  it('should display a running task', () => {
    notification.getToast().should('not.exist');

    // Check that running task is shown.
    notification.open();
    notification.getTasks().contains(poolName).should('exist');

    // Delete pool after task is complete (otherwise we get an error).
    notification.getTasks().should('not.exist');
  });

  it('should have notifications', () => {
    notification.open();
    notification.getNotifications().should('have.length.gt', 0);
  });

  it('should clear notifications', () => {
    notification.getToast().should('not.exist');
    notification.open();
    notification.getNotifications().should('have.length.gt', 0);
    notification.getClearNotificationsBtn().should('be.visible');
    notification.clearNotifications();
  });
});
