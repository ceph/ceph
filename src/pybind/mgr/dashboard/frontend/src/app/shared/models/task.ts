export class Task {
  constructor(name?: string, metadata?: object) {
    this.name = name;
    this.metadata = metadata;
  }
  name: string;
  metadata: object;

  description: string;
}
