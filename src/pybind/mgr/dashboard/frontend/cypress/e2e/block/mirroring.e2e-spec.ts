import { PoolPageHelper } from '../pools/pools.po';
import { MirroringPageHelper } from './mirroring.po';

describe('Mirroring page', () => {
  const pools = new PoolPageHelper();
  const mirroring = new MirroringPageHelper();

  beforeEach(() => {
    cy.login();
    mirroring.navigateTo();
  });

  it('should open and show breadcrumb', () => {
    mirroring.expectBreadcrumbText('Mirroring');
  });

  it('should show three tabs', () => {
    mirroring.getTabsCount().should('eq', 3);
  });

  it('should show text for all tabs', () => {
    mirroring.getTabText(0).should('eq', 'Issues (0)');
    mirroring.getTabText(1).should('eq', 'Syncing (0)');
    mirroring.getTabText(2).should('eq', 'Ready (0)');
  });

  describe('rbd mirroring bootstrap', () => {
    const poolName = 'rbd-mirror';

    beforeEach(() => {
      // login to the second ceph cluster
      cy.ceph2Login();
      cy.login();
      pools.navigateTo('create');
      pools.create(poolName, 8, 'rbd');
      pools.navigateTo();
      pools.existTableCell(poolName, true);
      mirroring.navigateTo();
    });

    it('should generate and import the bootstrap token between clusters', () => {
      const url: string = Cypress.env('CEPH2_URL');
      mirroring.navigateTo();
      mirroring.generateToken(poolName);
      cy.get('@token').then((bootstrapToken) => {
        // pass the token to the origin as an arg
        const args = { name: poolName, bootstrapToken: String(bootstrapToken) };
        // can't use any imports or functions inside the origin
        // so writing the code to copy the token inside the origin manually
        // rather than using a function call
        // @ts-ignore
        cy.origin(url, { args }, ({ name, bootstrapToken }) => {
          // Create an rbd pool in the second cluster

          // Login to the second cluster
          // Somehow its not working with the cypress login function
          cy.visit('#/pool/create').wait(100);

          cy.get('[name=username]').type('admin');
          cy.get('#password').type('admin');
          cy.get('[type=submit]').click();
          cy.get('input[name=name]').clear().type(name);
          cy.get(`select[name=poolType]`).select('replicated');
          cy.get(`select[name=poolType] option:checked`).contains('replicated');
          cy.get('.float-start.me-2.select-menu-edit').click();
          cy.get('.popover-body').should('be.visible');
          // Choose rbd as the application label
          cy.get('.select-menu-item-content').contains('rbd').click();
          cy.get('cd-submit-button').click();
          cy.get('cd-pool-list').should('exist');

          cy.visit('#/block/mirroring').wait(1000);
          cy.get('.table-actions button.dropdown-toggle').first().click();
          cy.get('[aria-label="Import Bootstrap Token"]').click();
          cy.get('cd-bootstrap-import-modal').within(() => {
            cy.get(`label[for=${name}]`).click();
            cy.get('textarea[id=token]').wait(100).type(bootstrapToken);
            cy.get('button[type=submit]').click();
          });
        });
      });

      // login again since origin removes all the cookies
      // sessions, localStorage items etc..
      cy.login();
      mirroring.navigateTo();
      mirroring.checkPoolHealthStatus(poolName, 'OK');
    });
  });

  describe('checks that edit mode functionality shows in the pools table', () => {
    const poolName = 'mirroring_test';

    beforeEach(() => {
      pools.navigateTo('create'); // Need pool for mirroring testing
      pools.create(poolName, 8, 'rbd');
      pools.navigateTo();
      pools.existTableCell(poolName, true);
    });

    it('tests editing mode for pools', () => {
      mirroring.navigateTo();

      mirroring.editMirror(poolName, 'Pool');
      mirroring.getFirstTableCell('pool').should('be.visible');
      mirroring.editMirror(poolName, 'Image');
      mirroring.getFirstTableCell('image').should('be.visible');
      mirroring.editMirror(poolName, 'Disabled');
      mirroring.getFirstTableCell('disabled').should('be.visible');
    });

    afterEach(() => {
      pools.navigateTo();
      pools.delete(poolName);
    });
  });
});
