import { Component, EventEmitter, Input, Output } from '@angular/core';
import { ComboBoxModule } from 'carbon-components-angular';
import { GroupsComboboxItem } from '~/app/shared/api/nvmeof.service';

@Component({
  selector: 'cd-nvmeof-gateway-group-filter',
  templateUrl: './nvmeof-gateway-group-filter.component.html',
  styleUrls: ['./nvmeof-gateway-group-filter.component.scss'],
  standalone: true,
  imports: [ComboBoxModule]
})
export class NvmeofGatewayGroupFilterComponent {
  @Input() items: GroupsComboboxItem[] = [];
  @Input() disabled = false;
  @Input() placeholder = $localize`Enter group name`;

  @Output() selected = new EventEmitter<GroupsComboboxItem>();
  @Output() cleared = new EventEmitter<void>();

  onSelected(item: GroupsComboboxItem): void {
    this.selected.emit(item);
  }

  onClear(): void {
    this.cleared.emit();
  }
}
