export class ImageSpec {
  static fromString(imageSpec: string) {
    const imageSpecSplited = imageSpec.split('/');

    const poolName = imageSpecSplited[0];
    const namespace = imageSpecSplited.length >= 3 ? imageSpecSplited[1] : null;
    const imageName = imageSpecSplited.length >= 3 ? imageSpecSplited[2] : imageSpecSplited[1];

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
