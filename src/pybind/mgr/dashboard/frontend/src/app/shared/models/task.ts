export class Task {
  constructor(name?, metadata?) {
    this.name = name;
    this.metadata = metadata;
  }
  name: string;
  metadata: object;

  description: string;
}
