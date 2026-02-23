import { PageHelper } from '../page-helper.po';

const pages = {
  list: { url: '#/cephfs/mirroring', id: 'cd-cephfs-mirroring-list' },
  paths: (fsName: string) => ({
    url: `#/cephfs/mirroring/${fsName}`,
    id: 'cd-cephfs-mirroring-paths'
  })
};

export class CephfsMirroringPageHelper extends PageHelper {
  pages = {
    list: pages.list,
    index: pages.list
  };

  navigateToList() {
    cy.visit(pages.list.url);
    cy.get(pages.list.id);
  }

  navigateToPaths(fsName: string) {
    const page = pages.paths(fsName);
    cy.visit(page.url);
    cy.get(page.id);
  }

  clickLocalFsLink(localFsName: string) {
    this.searchTable(localFsName);
    cy.contains('[cdstablerow] [cdstabledata] a', localFsName).click();
    cy.get(pages.paths(localFsName).id);
  }

  expectBreadcrumbSegments(segments: string[]) {
    cy.get('cds-breadcrumb-item').then(($items) => {
      const texts = $items.toArray().map((el) => el.textContent?.trim() ?? '');
      segments.forEach((segment, _) => {
        expect(texts).to.include(segment);
      });
    });
  }
}
