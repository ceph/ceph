import { PageHelper } from '../page-helper.po';

const pages = {
  index: { url: '#/rgw/roles', id: 'cd-crud-table' },
  create: { url: '#/rgw/roles/create', id: 'cd-crud-form' }
};

export class RolesPageHelper extends PageHelper {
  pages = pages;

  columnIndex = {
    roleName: 2,
    path: 3,
    arn: 4,
    createDate: 5,
    maxSessionDuration: 6
  };

  @PageHelper.restrictTo(pages.create.url)
  create(name: string, path: string, policyDocument: string) {
    cy.get('[id$="string_role_name_0"]').type(name);
    cy.get('[id$="role_assume_policy_doc_2"]').type(policyDocument);
    cy.get('[id$="role_path_1"]').type(path);
    cy.get("[aria-label='Create Role']").should('exist').click();
    cy.get('cd-crud-table').should('exist');
  }

  edit(name: string, maxSessionDuration: number) {
    this.navigateEdit(name);
    cy.get('[id$="max_session_duration_1"]').clear().type(maxSessionDuration.toString());
    cy.get("[aria-label='Edit Role']").should('exist').click();
    cy.get('cd-crud-table').should('exist');

    this.getTableCell(this.columnIndex.roleName, name)
      .click()
      .parent()
      .find(`datatable-body-cell:nth-child(${this.columnIndex.maxSessionDuration})`)
      .should(($elements) => {
        const roleName = $elements.map((_, el) => el.textContent).get();
        expect(roleName).to.include(`${maxSessionDuration} hours`);
      });
  }

  @PageHelper.restrictTo(pages.index.url)
  checkExist(name: string, exist: boolean) {
    this.getTableCell(this.columnIndex.roleName, name).should(($elements) => {
      const roleName = $elements.map((_, el) => el.textContent).get();
      if (exist) {
        expect(roleName).to.include(name);
      } else {
        expect(roleName).to.not.include(name);
      }
    });
  }
}
