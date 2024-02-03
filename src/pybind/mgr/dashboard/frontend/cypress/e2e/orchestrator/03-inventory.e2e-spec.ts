import { InventoryPageHelper } from '../cluster/inventory.po';

describe('Physical Disks page', () => {
  const inventory = new InventoryPageHelper();

  beforeEach(() => {
    cy.login();
    inventory.navigateTo();
  });

  it('should have correct devices', () => {
    cy.fixture('orchestrator/inventory.json').then((hosts) => {
      const totalDiskCount = Cypress._.sumBy(hosts, 'devices.length');
      inventory.expectTableCount('total', totalDiskCount);
      for (const host of hosts) {
        inventory.filterTable('Hostname', host['name']);
        inventory.getTableCount('found').should('be.eq', host.devices.length);
      }
    });
  });

  it('should identify device', () => {
    inventory.identify();
  });
});
