import { InventoryPageHelper } from '../../cluster/inventory.po';

describe('Physical Disks page', () => {
  const inventory = new InventoryPageHelper();

  const orchestratorStatusWithIdentify = {
    available: true,
    message: '',
    features: {
      blink_device_light: { available: true },
      get_inventory: { available: true }
    }
  };

  beforeEach(() => {
    cy.login();
    cy.intercept('GET', '**/ui-api/orchestrator/status', orchestratorStatusWithIdentify);
    inventory.navigateTo();
  });

  it('should identify device', () => {
    inventory.identify();
  });
});
