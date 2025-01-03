import { PageHelper } from '../page-helper.po';

export class DashboardV3PageHelper extends PageHelper {
  pages = { index: { url: '#/dashboard', id: 'cd-dashboard-v3' } };

  cardTitle(index: number) {
    return cy.get('.card-title').its(index).text();
  }

  clickInventoryCardLink(link: string) {
    console.log(link);
    cy.get(`cd-card[cardTitle="Inventory"]`).contains('a', link).click();
  }

  card(indexOrTitle: number) {
    cy.get('cd-card').as('cards');

    return cy.get('@cards').its(indexOrTitle);
  }
}
