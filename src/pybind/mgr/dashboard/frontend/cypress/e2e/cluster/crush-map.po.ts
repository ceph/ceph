import { PageHelper } from '../page-helper.po';

export class CrushMapPageHelper extends PageHelper {
  pages = { index: { url: '#/crush-map', id: 'cd-crushmap' } };

  getPageTitle() {
    return cy.get('cd-crushmap .card-header').text();
  }

  getCrushNode(idx: number) {
    return cy.get('.node-name.type-osd').eq(idx);
  }
}
