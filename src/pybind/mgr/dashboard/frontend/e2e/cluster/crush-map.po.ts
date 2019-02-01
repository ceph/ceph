import { browser } from 'protractor';

export class CrushMapPage {
  navigateTo() {
    return browser.get('/#/crush-map');
  }
}
