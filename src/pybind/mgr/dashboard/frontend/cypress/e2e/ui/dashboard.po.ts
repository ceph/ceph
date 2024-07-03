import { PageHelper } from '../page-helper.po';

export class DashboardPageHelper extends PageHelper {
  pages = { index: { url: '#/dashboard', id: 'cd-dashboard' } };

  infoGroupTitle(index: number) {
    return cy.get('.info-group-title').its(index).text();
  }

  clickInfoCardLink(cardName: string) {
    cy.get(`cd-info-card[cardtitle="${cardName}"]`).contains('a', cardName).click();
  }

  infoCard(indexOrTitle: number | string) {
    cy.get('cd-info-card').as('infoCards');

    if (typeof indexOrTitle === 'number') {
      return cy.get('@infoCards').its(indexOrTitle);
    } else {
      return cy.contains('cd-info-card a', indexOrTitle).parent().parent().parent().parent();
    }
  }

  infoCardBodyText(infoCard: string) {
    return this.infoCard(infoCard).find('.card-text').text();
  }

  infoCardBody(infoCard: string) {
    return this.infoCard(infoCard).find('.card-text');
  }
}
