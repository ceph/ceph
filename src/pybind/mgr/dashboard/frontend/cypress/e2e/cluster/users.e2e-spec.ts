import { UsersPageHelper } from './users.po';

describe('Cluster Ceph Users', () => {
  const users = new UsersPageHelper();

  beforeEach(() => {
    cy.login();
    users.navigateTo();
  });

  describe('breadcrumb and tab tests', () => {
    it('should open and show breadcrumb', () => {
      users.expectBreadcrumbText('Ceph Users');
    });
  });

  describe('Cluster users table', () => {
    const entityName = 'client.test';
    const entity = 'mgr';
    const caps = 'allow r';
    it('should verify the table is not empty', () => {
      users.checkForUsers();
    });

    it('should verify the keys are hidden', () => {
      users.verifyKeysAreHidden();
    });

    it('should create a new user', () => {
      users.navigateTo('create');
      users.create(entityName, entity, caps);
      users.existTableCell(entityName, true);
    });

    it('should edit a user', () => {
      const newCaps = 'allow *';
      users.edit(entityName, 'allow *');
      users.existTableCell(entityName, true);
      users.checkCaps(entityName, [`${entity}: ${newCaps}`]);
    });

    it('should delete a user', () => {
      users.delete(entityName);
    });
  });
});
