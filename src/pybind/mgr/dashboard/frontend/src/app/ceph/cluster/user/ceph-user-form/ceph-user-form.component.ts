import { Component } from '@angular/core';
import { FormBuilder, FormGroup, FormArray, Validators } from '@angular/forms';

@Component({
  selector: 'cd-ceph-user-form',
  templateUrl: './ceph-user-form.component.html',
  styleUrls: ['./ceph-user-form.component.scss'],
  standalone: false
})
export class CephUserFormComponent {
  form: FormGroup;

  constructor(private fb: FormBuilder) {
    this.form = this.fb.group({
      username: ['', Validators.required],
      caps: this.fb.array([this.createCap()])
    });
  }

  get caps(): FormArray {
    return this.form.get('caps') as FormArray;
  }

  createCap(): FormGroup {
    return this.fb.group({
      entity: ['', Validators.required],
      permission: ['allow rw', Validators.required]
    });
  }

  addCap(): void {
    this.caps.push(this.createCap());
  }

  removeCap(index: number): void {
    if (this.caps.length > 1) {
      this.caps.removeAt(index);
    }
  }

  onSubmit(): void {
    if (this.form.invalid) {
      return;
    }
    const formValue = this.form.value;

    const payload = {
      user: `client.${formValue.username.trim()}`,
      caps: formValue.caps.map(
        (c: { entity: string; permission: string }) => `${c.entity} ${c.permission}`
      )
    };
    console.debug(payload);
    // TODO: integrate with API service
  }
}
