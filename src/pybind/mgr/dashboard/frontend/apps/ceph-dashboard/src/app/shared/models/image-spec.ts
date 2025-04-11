export class ImageSpec {
  static fromString(imageSpec: string) {
    const imageSpecSplit = imageSpec.split('/');

    const poolName = imageSpecSplit[0];
    const namespace = imageSpecSplit.length >= 3 ? imageSpecSplit[1] : null;
    const imageName = imageSpecSplit.length >= 3 ? imageSpecSplit[2] : imageSpecSplit[1];

    return new this(poolName, namespace, imageName);
  }

  constructor(public poolName: string, public namespace: string, public imageName: string) {}

  private getNameSpace() {
    return this.namespace ? `${this.namespace}/` : '';
  }

  toString() {
    return `${this.poolName}/${this.getNameSpace()}${this.imageName}`;
  }

  toStringEncoded() {
    return encodeURIComponent(`${this.poolName}/${this.getNameSpace()}${this.imageName}`);
  }
}
