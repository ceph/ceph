export class User {
  username: string;
  password: string;

  setValues(values: any) {
    this.username = values.username;
    this.password = values.password;
  }
}
