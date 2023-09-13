export abstract class ApiClient {
  getVersionHeaderValue(major: number, minor: number) {
    return `application/vnd.ceph.api.v${major}.${minor}+json`;
  }
}
