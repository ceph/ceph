import { RolesPageHelper } from './roles.po';

describe('RGW roles page', () => {
  const roles = new RolesPageHelper();

  beforeEach(() => {
    cy.login();
    roles.navigateTo();
  });

  describe('Create, Edit & Delete rgw roles', () => {
    const roleName = 'testRole';

    it('should create rgw roles', () => {
      roles.navigateTo('create');
      roles.create(roleName, '/', '{}');
      roles.navigateTo();
      roles.checkExist(roleName, true);
    });

    it('should edit rgw role', () => {
      roles.edit(roleName, 3);
    });

    it('should delete rgw role', () => {
      roles.delete(roleName);
    });
  });
});
