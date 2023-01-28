import { PageHelper } from '../page-helper.po';

const pages = {
  index: { url: '#/ceph-users', id: 'cd-crud-table' }
};

export class UsersPageHelper extends PageHelper {
  pages = pages;

  columnIndex = {
    entity: 1,
    capabilities: 2,
    key: 3
  };

  checkForUsers() {
    this.getTableCount('total').should('not.be.eq', 0);
  }

  verifyKeysAreHidden() {
    this.getTableCell(this.columnIndex.entity, 'osd.0')
      .parent()
      .find(`datatable-body-cell:nth-child(${this.columnIndex.key}) span`)
      .should(($ele) => {
        const serviceInstances = $ele.toArray().map((v) => v.innerText);
        expect(serviceInstances).not.contains(/^[a-z0-9]+$/i);
      });
  }
}
