import { PageHelper } from '../page-helper.po';

export class NotificationSidebarPageHelper extends PageHelper {
  getNotificatinoIcon() {
    return cy.get('cd-notifications a');
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

  getClearNotficationsBtn() {
    return this.getSidebar().find('button.btn-block');
  }

  getCloseBtn() {
    return this.getSidebar().find('button.close');
  }

  open() {
    this.getNotificatinoIcon().click();
    this.getSidebar().should('be.visible');
  }

  clearNotifications() {
    // It can happen that although notifications are cleared, by the time we check the notifications
    // amount, another notification can appear, so we check it more than once (if needed).
    this.getClearNotficationsBtn().click();
    this.getNotifications()
      .should('have.length.gte', 0)
      .then(($elems) => {
        if ($elems.length > 0) {
          this.clearNotifications();
        }
      });
  }
}
