import { by, element } from 'protractor';

import { PageHelper } from '../page-helper.po';

export class NotificationSidebarPageHelper extends PageHelper {
  getNotificatinoIcon() {
    return element(by.css('cd-notifications a'));
  }

  getSidebar() {
    return element(by.css('aside.ng-sidebar--opened cd-notifications-sidebar'));
  }

  getTasks() {
    return this.getSidebar().all(by.css('.card.tc_task'));
  }

  getNotifications() {
    return this.getSidebar().all(by.css('.card.tc_notification'));
  }

  getClearNotficationsBtn() {
    return this.getSidebar().element(by.css('button.btn-block'));
  }

  getCloseBtn() {
    return this.getSidebar().element(by.css('button.close'));
  }

  async open() {
    await this.waitClickableAndClick(this.getNotificatinoIcon());
    return this.waitVisibility(this.getSidebar());
  }
}
