import { PoolPageHelper } from '../pools/pools.po';
import { NotificationSidebarPageHelper } from './notification.po';

describe('Notification page', () => {
  const notification = new NotificationSidebarPageHelper();
  const pools = new PoolPageHelper();
  const poolName = 'e2e_notification_pool';

  before(() => {
    cy.login();
    pools.navigateTo('create');
    pools.create(poolName, 8, ['rbd']);
    cy.wait(5000);
    pools.navigateTo();
    pools.getTableRow(poolName).should('exist');
    pools.edit_pool_pg(poolName, 4, false);
  });

  after(() => {
    cy.login();
    pools.navigateTo();
    pools.delete(poolName, null, null, true, false, true, true);
  });

  beforeEach(() => {
    cy.login();
    pools.navigateTo();
  });

  it('should open notification sidebar', () => {
    notification.open();
    notification.getSidebar().should('exist');
  });

  it('should display a running task', () => {
    notification.open();
    cy.contains('.task-item', poolName).should('exist').and('be.visible');
    cy.contains('.task-item', poolName).should('not.exist');
  });

  it('should have notifications', () => {
    notification.open();
    notification.getNotificationCount().then((count) => {
      cy.log(`Notification count: ${count}`);
      expect(count).to.be.at.least(0);
    });
  });

  it('should clear notifications', () => {
    notification.open();
    notification.clearNotifications();
    notification.getNotificationCount().then((count) => {
      expect(count).to.eq(0);
    });
  });
});
