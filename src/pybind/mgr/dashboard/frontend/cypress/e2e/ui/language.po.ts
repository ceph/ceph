import { PageHelper } from '../page-helper.po';

export class LanguagePageHelper extends PageHelper {
  pages = {
    index: { url: '#/dashboard', id: 'cd-dashboard' }
  };

  getLanguageBtn() {
    return cy.get('cd-language-selector cds-header-menu a').first();
  }

  getAllLanguages() {
    return cy.get('cd-language-selector cds-header-menu cds-header-item');
  }
}
