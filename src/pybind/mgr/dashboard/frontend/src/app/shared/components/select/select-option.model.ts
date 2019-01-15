export class SelectOption {
  selected: boolean;
  name: string;
  description: string;

  constructor(selected: boolean, name: string, description: string) {
    this.selected = selected;
    this.name = name;
    this.description = description;
  }
}
