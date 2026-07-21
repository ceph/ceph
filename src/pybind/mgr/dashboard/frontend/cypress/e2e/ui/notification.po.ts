import { PageHelper } from '../page-helper.po';

export class NotificationSidebarPageHelper extends PageHelper {
  pages = {
    index: { url: '#/notifications', id: 'cd-notifications-page' }
  };

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

  getViewAllBtn() {
    return cy.get('.notification-footer__view-all-button');
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

  // Notifications page helpers

  getNotificationsPage() {
    return cy.get('.notifications-page');
  }

  getNotificationsPageList() {
    return cy.get('.notifications-page__list');
  }

  getNotificationsPageListItems() {
    return cy.get('.notifications-page__item');
  }

  getNotificationsPageDetail() {
    return cy.get('.notifications-page__detail');
  }

  getKebabMenu() {
    return cy.get('.notifications-page__header-menu cds-overflow-menu');
  }

  getKebabMenuItems() {
    return cy.get('.cds--overflow-menu-options__btn');
  }

  getOccurrencesLabel() {
    return cy.get('.cd-notification-item__occurrences');
  }

  getToastContainer() {
    return cy.get('.cds--toast-notification-container');
  }

  getToasts() {
    return cy.get('cds-toast');
  }

  getToastViewMoreLink() {
    return cy.get('.toast-view-more');
  }

  getToastDuplicateCount() {
    return cy.get('.toast-duplicate-count');
  }

  navigateToNotificationsPage() {
    this.open();
    this.getViewAllBtn().click({ force: true });
    this.getNotificationsPage().should('exist');
  }
}
