import { PageHelper } from '../page-helper.po';

const pages = {
  index: { url: '#/ceph-users', id: 'cd-crud-table' },
  create: { url: '#/cluster/user/create', id: 'cd-crud-form' }
};

export class UsersPageHelper extends PageHelper {
  pages = pages;

  columnIndex = {
    entity: 2,
    capabilities: 3,
    key: 4
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

  @PageHelper.restrictTo(pages.create.url)
  create(entityName: string, entityType: string, caps: string) {
    cy.get('#formly_2_string_user_entity_0').type(entityName);
    cy.get('#formly_5_string_entity_0').type(entityType);
    cy.get('#formly_5_string_cap_1').type(caps);
    cy.get("[aria-label='Create User']").should('exist').click();
    cy.get('cd-crud-table').should('exist');
  }

  edit(name: string, newCaps: string) {
    this.navigateEdit(name);
    cy.get('#formly_5_string_cap_1').clear().type(newCaps);
    cy.get("[aria-label='Edit User']").should('exist').click();
    cy.get('cd-crud-table').should('exist');
  }

  checkCaps(entityName: string, capabilities: string[]) {
    this.getTableCell(this.columnIndex.entity, entityName)
      .click()
      .parent()
      .find(`datatable-body-cell:nth-child(${this.columnIndex.capabilities}) .badge`)
      .should(($ele) => {
        const newCaps = $ele.toArray().map((v) => v.innerText);
        for (const cap of capabilities) {
          expect(newCaps).to.include(cap);
        }
      });
  }
}
