/* eslint-disable @typescript-eslint/ban-types */
export class Task {
  name: string;
  metadata: Record<string, unknown>;

  description: string;

  constructor(name?: string, metadata?: Record<string, unknown>) {
    this.name = name;
    this.metadata = metadata;
  }
}
