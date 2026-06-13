import { PageHelper } from '../page-helper.po';

export class NotificationSidebarPageHelper extends PageHelper {
  getNotificationIcon() {
    return cy.get(`[data-testid='header-notification-icon']`);
  }

  getPanel() {
    return cy.get('cd-notification-panel');
  }

  getSidebar() {
    return cy.get('cd-notification-area');
  }

  getTasks() {
    return cy.get('cd-notification-area .task-item');
  }

  getNotifications() {
    return cy.get('cd-notification-area [data-testid="notification-item"]');
  }

  getNotificationCount() {
    return cy.get('cd-notification-area').then(($area) => {
      return $area.find('[data-testid="notification-item"]').length;
    });
  }

  getClearNotificationsBtn() {
    return cy.get('cd-notification-panel .notification-header__dismiss-btn');
  }

  open() {
    this.getNotificationIcon().click({ force: true });
    this.getPanel().should('exist');
    this.getSidebar().should('exist');
  }

  clearNotifications() {
    this.getNotificationCount().then((count) => {
      if (count === 0) {
        return;
      }

      this.getClearNotificationsBtn().scrollIntoView().click({ force: true });

      this.clearNotifications();
    });
  }
}
