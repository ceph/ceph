import { InventoryPageHelper } from '../../cluster/inventory.po';

describe('Physical Disks page', () => {
  const inventory = new InventoryPageHelper();

  beforeEach(() => {
    cy.login();
    inventory.navigateTo();
  });

  it('should identify device', () => {
    inventory.identify();
  });
});
