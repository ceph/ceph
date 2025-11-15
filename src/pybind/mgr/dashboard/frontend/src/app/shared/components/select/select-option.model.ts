export class SelectOption {
  selected: boolean;
  name: string;
  description: string;
  enabled: boolean;
  content?: string;

  constructor(selected: boolean, name: string, description: string, enabled = true) {
    this.selected = selected;
    this.name = name;
    this.description = description;
    this.enabled = enabled;
    this.content = name;
  }
}
