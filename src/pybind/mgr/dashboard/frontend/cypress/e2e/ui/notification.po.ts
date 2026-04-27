import { PageHelper } from '../page-helper.po';

export class NotificationSidebarPageHelper extends PageHelper {
  getNotificationIcon() {
    return cy.get(`[data-testid='header-notification-icon']`);
  }

  getSidebar() {
    return cy.get('cd-notifications-sidebar');
  }

  getTasks() {
    return this.getSidebar().find('.card.tc_task');
  }

  getNotifications() {
    return this.getSidebar().find('.card.tc_notification');
  }

  getClearNotificationsBtn() {
    return this.getSidebar().find('button.btn-block');
  }

  getCloseBtn() {
    return this.getSidebar().find('button.close');
  }

  open() {
    this.getNotificationIcon().click({ force: true });
    this.getPanel().should('exist');
    this.getSidebar().should('exist');
  }

  clearNotifications() {
    // It can happen that although notifications are cleared, by the time we check the notifications
    // amount, another notification can appear, so we check it more than once (if needed).
    this.getClearNotificationsBtn().click();
    this.getNotifications()
      .should('have.length.gte', 0)
      .then(($elems) => {
        if ($elems.length > 0) {
          this.clearNotifications();
        }
      });
  }
}
