import { LanguagePageHelper } from './language.po';

describe('Shared pages', () => {
  const language = new LanguagePageHelper();

  beforeEach(() => {
    cy.login();
    language.navigateTo();
  });

  it('should check default language', () => {
    language.getLanguageBtn().should('contain.text', 'English');
  });

  it('should check all available languages', () => {
    language.getLanguageBtn().click();
    language.getAllLanguages().should('have.length', 1).should('contain.text', 'English');
  });
});
