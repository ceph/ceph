import { PageHelper } from '../page-helper.po';

export class OverviewPagehelper extends PageHelper {
  pages = { index: { url: '#/overview', id: 'cd-overview' } };

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

  clickSystemsTab() {
    cy.get(`[data-test-id="systems-tab"]`).click();
  }

  cardRow(rowName: string) {
    return cy.get(`[data-testid=${rowName}]`);
  }
}
