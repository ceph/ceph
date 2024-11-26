export class CdsComboBoxOption {
    selected: boolean;
    content: string;
    description: string;
    enabled: boolean;
  
    constructor(selected: boolean, content: string, description: string , enabled = true) {
      this.selected = selected;
      this.content = content;
      this.description = description;
      this.enabled = enabled;
    }
  }