import { InventoryPageHelper } from '../../cluster/inventory.po';

describe('Physical Disks page', () => {
  const inventory = new InventoryPageHelper();

  beforeEach(() => {
    cy.login();
    cy.intercept('GET', '**/ui-api/orchestrator/status', {
      body: {
        available: true,
        message: null,
        features: {
          blink_device_light: { available: true }
        }
      }
    });
    inventory.navigateTo();
  });

  it('should identify device', () => {
    inventory.identify();
  });
});
