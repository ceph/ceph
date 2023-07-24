import { RolesPageHelper } from './roles.po';

describe('RGW roles page', () => {
  const roles = new RolesPageHelper();

  beforeEach(() => {
    cy.login();
    roles.navigateTo();
  });

  describe('Create, Edit & Delete rgw roles', () => {
    it('should create rgw roles', () => {
      roles.navigateTo('create');
      roles.create('testRole', '/', '{}');
      roles.navigateTo();
      roles.checkExist('testRole', true);
    });
  });
});
