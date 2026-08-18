import { PageHelper } from '../page-helper.po';

const pages = {
  cephfsMirroring: { url: '#/cephfs/mirroring', id: 'cd-cephfs-mirroring-list' }
};

export class PageHeaderPageHelper extends PageHelper {
  pages = pages;

  navigateToCephfsMirroring() {
    cy.visit(pages.cephfsMirroring.url);
    cy.get(pages.cephfsMirroring.id, { timeout: 10000 });
  }

  getPageHeader() {
    return cy.get('[data-testid="page-header"]');
  }

  getHeaderTitle() {
    return this.getPageHeader().find('.cds--type-heading-04').invoke('text');
  }

  getHeaderDescription() {
    return this.getPageHeader().find('.cds--type-body-01').invoke('text');
  }
}
