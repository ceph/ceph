import { $, $$ } from 'protractor';
import { PageHelper } from '../page-helper.po';

export class CrushMapPageHelper extends PageHelper {
  pages = { index: '/#/crush-map' };

  getPageTitle() {
    return $('cd-crushmap .card-header').getText();
  }

  getCrushNode(idx) {
    return $$('.node-name.type-osd').get(idx);
  }
}
