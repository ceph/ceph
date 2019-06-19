import { $, $$, browser } from 'protractor';

interface Pages {
  index: string;
}

export abstract class PageHelper {
  pages: Pages;

  static getBreadcrumbText() {
    return $('.breadcrumb-item.active').getText();
  }

  static getTabText(idx) {
    return $$('.nav.nav-tabs li')
      .get(idx)
      .getText();
  }

  static getTabsCount() {
    return $$('.nav.nav-tabs li').count();
  }

  navigateTo(page = null) {
    return browser.get(this.pages[page || 'index']);
  }
}
