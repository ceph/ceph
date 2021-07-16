describe('Login Page', () => {
  beforeEach(() => {
    cy.visit('#/login');
    cy.eyesOpen({
      appName: 'Ceph',
      testName: 'Login Component Check'
    });
  });

  afterEach(() => {
    cy.eyesClose();
  });

  it('types login credentials and takes screenshot', () => {
    cy.get('[name=username]').type('admin');
    cy.get('#password').type('admin');
    cy.eyesCheckWindow({ tag: 'Login Screen with credentials typed' });
  });
});
