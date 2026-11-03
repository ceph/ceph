import { PageHelper } from '../page-helper.po';

export class DaemonsPageHelper extends PageHelper {
  pages = {
    index: { url: '#/rgw/daemon', id: 'cd-rgw-daemon-list' }
  };

  getOverviewField(label: string) {
    return cy
      .get('.cd-overview-label')
      .filter((_index, el) => el.textContent?.includes(label))
      .closest('.cd-overview-item');
  }

  checkResourcePage() {
    // Click a daemon row link so the resource page is opened.
    this.getResourcePage().click();

    cy.get('cd-resource-overview-card').should('be.visible');
    this.getOverviewField('Ceph Version').should('be.visible');

    // Check the Performance tab is accessible and the URL hash changes accordingly.
    cy.contains('cds-sidenav-item a', /^Performance$/).click();
    cy.location('hash').should('include', '/performance');
  }
}
