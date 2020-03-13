import { RoleMgmtPageHelper } from './role-mgmt.po';

describe('Role Management page', () => {
  const roleMgmt = new RoleMgmtPageHelper();
  const role_name = 'e2e_role_mgmt_role';

  beforeEach(() => {
    cy.login();
    roleMgmt.navigateTo();
  });

  describe('breadcrumb tests', () => {
    it('should check breadcrumb on roles tab on user management page', () => {
      roleMgmt.expectBreadcrumbText('Roles');
    });

    it('should check breadcrumb on role creation page', () => {
      roleMgmt.navigateTo('create');
      roleMgmt.expectBreadcrumbText('Create');
    });
  });

  describe('role create, edit & delete test', () => {
    it('should create a role', () => {
      roleMgmt.create(role_name, 'An interesting description');
    });

    it('should edit a role', () => {
      roleMgmt.edit(role_name, 'A far more interesting description');
    });

    it('should delete a role', () => {
      roleMgmt.delete(role_name);
    });
  });
});
