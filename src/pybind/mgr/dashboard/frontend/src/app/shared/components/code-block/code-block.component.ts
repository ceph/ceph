import { Component, Input } from '@angular/core';

@Component({
  selector: 'cd-code-block',
  templateUrl: './code-block.component.html',
  styleUrls: ['./code-block.component.scss']
})
export class CodeBlockComponent {
  @Input()
  codes: string[];
}
