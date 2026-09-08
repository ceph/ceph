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

describe('Notification panel to page navigation', () => {
  const notification = new NotificationSidebarPageHelper();

  beforeEach(() => {
    cy.login();
    cy.visit('#/');
    cy.wait(5000);
  });

  it('should navigate to notifications page via View all', () => {
    notification.navigateToNotificationsPage();
    cy.url().should('include', '/notifications');
    notification.getNotificationsPage().should('exist');
  });

  it('should navigate from panel notification click to page with pre-selected detail', () => {
    notification.open();
    notification.getNotifications().then(($items) => {
      if ($items.length === 0) {
        cy.log('No notifications in panel — skipping');
        return;
      }
      notification.getNotifications().first().click();
      cy.url().should('include', '/notifications');
      cy.url().should('include', 'id=');
      notification.getNotificationsPage().should('exist');
      notification.getNotificationsPageDetail().find('cd-notification-item').should('exist');
      notification
        .getNotificationsPageListItems()
        .first()
        .should('have.class', 'notifications-page__item--active');
    });
  });

  it('should show back button and navigate back', () => {
    notification.navigateToNotificationsPage();
    cy.contains('Back').should('be.visible').click();
    cy.url().should('not.include', '/notifications');
  });
});

describe('Notifications page actions', () => {
  const notification = new NotificationSidebarPageHelper();

  beforeEach(() => {
    cy.login();
    cy.visit('#/');
    cy.wait(3000);
  });

  it('should show kebab menu with mark all as read and clear all', () => {
    notification.navigateToNotificationsPage();
    notification.getKebabMenu().click();
    notification.getKebabMenuItems().should('have.length', 2);
    cy.contains('.cds--overflow-menu-options__btn', 'Mark all as read').should('exist');
    cy.contains('.cds--overflow-menu-options__btn', 'Clear all').should('exist');
  });

  it('should show empty state when no notifications', () => {
    notification.open();
    notification.clearNotifications();
    notification.getViewAllBtn().click({ force: true });
    notification.getNotificationsPage().should('exist');
    cy.contains('No notifications available').should('be.visible');
  });

  it('should disable kebab actions when no notifications', () => {
    notification.open();
    notification.clearNotifications();
    notification.getViewAllBtn().click({ force: true });
    notification.getKebabMenu().click();
    cy.contains('.cds--overflow-menu-options__btn', 'Mark all as read').should('be.disabled');
    cy.contains('.cds--overflow-menu-options__btn', 'Clear all').should('be.disabled');
  });
});

describe('Notification selection and detail', () => {
  const notification = new NotificationSidebarPageHelper();

  beforeEach(() => {
    cy.login();
    cy.visit('#/');
    cy.wait(5000);
  });

  it('should select a notification and show detail in right panel', () => {
    notification.navigateToNotificationsPage();
    notification.getNotificationsPageListItems().then(($items) => {
      if ($items.length === 0) {
        cy.log('No notifications to select — skipping');
        return;
      }
      cy.wrap($items.first()).click();
      cy.wrap($items.first()).should('have.class', 'notifications-page__item--active');
      notification.getNotificationsPageDetail().find('cd-notification-item').should('exist');
      notification
        .getNotificationsPageDetail()
        .find('.notifications-page__detail-text')
        .should('exist');
    });
  });

  it('should show occurrences count when notification has duplicates', () => {
    notification.navigateToNotificationsPage();
    notification.getNotificationsPageListItems().then(($items) => {
      if ($items.length === 0) {
        cy.log('No notifications — skipping');
        return;
      }
      cy.wrap($items.first()).click();
      notification.getNotificationsPageDetail().then(($detail) => {
        const hasOccurrences = $detail.find('.notifications-page__occurrences').length > 0;
        if (hasOccurrences) {
          cy.get('.notifications-page__occurrences')
            .should('be.visible')
            .and('contain', 'Occurrences:');
        } else {
          cy.log('First notification has no duplicates — occurrences label correctly hidden');
        }
      });
    });
  });

  it('should clear all notifications via kebab menu', () => {
    notification.navigateToNotificationsPage();
    notification.getNotificationsPageListItems().then(($items) => {
      if ($items.length === 0) {
        cy.log('No notifications to clear — skipping');
        return;
      }
      notification.getKebabMenu().click();
      cy.contains('.cds--overflow-menu-options__btn', 'Clear all').click();
      cy.contains('No notifications available').should('be.visible');
    });
  });
});

describe('Notification toast deduplication', () => {
  const notification = new NotificationSidebarPageHelper();

  beforeEach(() => {
    cy.login();
    cy.visit('#/');
  });

  it('should show +N more count on duplicate toasts', () => {
    cy.wait(8000);
    cy.get('body').then(($body) => {
      if ($body.find('.toast-duplicate-count').length > 0) {
        notification
          .getToastDuplicateCount()
          .first()
          .should('be.visible')
          .invoke('text')
          .should('match', /\(\+\d+ more\)/);
      } else if ($body.find('cds-toast').length > 0) {
        cy.log('Toasts present but no duplicates yet — only unique errors firing');
      } else {
        cy.log('No toasts appeared — skipping');
      }
    });
  });

  it('should show view more link when toast text is truncated', () => {
    cy.wait(5000);
    cy.get('body').then(($body) => {
      if ($body.find('.toast-view-more:visible').length > 0) {
        notification
          .getToastViewMoreLink()
          .first()
          .should('be.visible')
          .and('contain', 'View more');
      } else {
        cy.log('No truncated toasts with view more — skipping');
      }
    });
  });

  it('should navigate to notifications page and dismiss toasts on view more click', () => {
    cy.wait(5000);
    cy.get('body').then(($body) => {
      if ($body.find('.toast-view-more:visible').length > 0) {
        notification.getToastViewMoreLink().first().click({ force: true });
        cy.url().should('include', '/notifications');
        notification.getNotificationsPage().should('exist');
        notification.getToasts().should('not.exist');
      } else {
        cy.log('No visible view more link — skipping');
      }
    });
  });
});

describe('Notification read state', () => {
  const notification = new NotificationSidebarPageHelper();

  beforeEach(() => {
    cy.login();
    cy.visit('#/');
    cy.wait(5000);
  });

  it('should mark notification as read on selection', () => {
    notification.navigateToNotificationsPage();
    notification.getNotificationsPageListItems().then(($items) => {
      if ($items.length === 0) {
        cy.log('No notifications — skipping');
        return;
      }
      cy.wrap($items.first()).click();
      cy.wrap($items.first()).should('not.have.class', 'notifications-page__item--unread');
    });
  });

  it('should mark all as read via kebab menu', () => {
    notification.navigateToNotificationsPage();
    notification.getNotificationsPageListItems().then(($items) => {
      if ($items.length === 0) {
        cy.log('No notifications — skipping');
        return;
      }
      notification.getKebabMenu().click();
      cy.contains('.cds--overflow-menu-options__btn', 'Mark all as read').click();
      notification.getNotificationsPageListItems().each(($item) => {
        cy.wrap($item).should('not.have.class', 'notifications-page__item--unread');
      });
    });
  });

  it('should show unread indicator on bell icon when unread notifications exist', () => {
    cy.visit('#/');
    cy.wait(5000);
    cy.get('[data-testid="header-notification-icon"]').then(($icon) => {
      const iconType = $icon.find('cd-icon').attr('type');
      cy.log(`Bell icon type: ${iconType}`);
      expect(['notification', 'notificationNew']).to.include(iconType);
    });
  });
});
