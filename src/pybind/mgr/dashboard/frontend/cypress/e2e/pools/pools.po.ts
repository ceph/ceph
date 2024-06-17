import { PageHelper } from '../page-helper.po';

const pages = {
  index: { url: '#/pool', id: 'cd-pool-list' },
  create: { url: '#/pool/create', id: 'cd-pool-form' }
};

export class PoolPageHelper extends PageHelper {
  pages = pages;

  private isPowerOf2(n: number) {
    // tslint:disable-next-line: no-bitwise
    return expect((n & (n - 1)) === 0, `Placement groups ${n} are not a power of 2`).to.be.true;
  }

  @PageHelper.restrictTo(pages.create.url)
  create(name: string, placement_groups: number, ...apps: string[]) {
    cy.get('input[name=name]').clear().type(name);

    this.isPowerOf2(placement_groups);

    this.selectOption('poolType', 'replicated');

    this.expectSelectOption('pgAutoscaleMode', 'on');
    this.selectOption('pgAutoscaleMode', 'off'); // To show pgNum field
    cy.get('input[name=pgNum]').clear().type(`${placement_groups}`);
    this.setApplications(apps);
    cy.get('#rbdMirroring').check({ force: true });
    cy.get('cd-submit-button').click();
  }

  edit_pool_pg(name: string, new_pg: number, wait = true) {
    this.isPowerOf2(new_pg);
    this.navigateEdit(name);

    cy.get('#rbdMirroring').should('be.checked');

    cy.get('input[name=pgNum]').clear().type(`${new_pg}`);
    cy.get('cd-submit-button').click();
    const str = `${new_pg} active+clean`;
    this.getTableRow(name);
    if (wait) {
      this.getTableRow(name).contains(str);
    }
  }

  edit_pool_configuration(name: string, bpsLimit: string) {
    this.navigateEdit(name);

    cy.get('cd-rbd-configuration-form')
      .get('input[name=rbd_qos_bps_limit]')
      .clear()
      .type(`${bpsLimit}`);
    cy.get('cd-submit-button').click();

    this.navigateEdit(name);

    cy.get('cd-rbd-configuration-form')
      .get('input[name=rbd_qos_bps_limit]')
      .should('have.value', bpsLimit);
  }

  private setApplications(apps: string[]) {
    if (!apps || apps.length === 0) {
      return;
    }
    cy.get('.float-start.me-2.select-menu-edit').click();
    cy.get('.popover-body').should('be.visible');
    apps.forEach((app) => cy.get('.select-menu-item-content').contains(app).click());
  }
}
