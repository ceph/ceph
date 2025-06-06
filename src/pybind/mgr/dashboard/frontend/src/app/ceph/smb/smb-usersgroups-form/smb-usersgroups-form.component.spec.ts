import { ComponentFixture, TestBed } from '@angular/core/testing';

import { SmbUsersgroupsFormComponent } from './smb-usersgroups-form.component';
import { ToastrModule } from 'ngx-toastr';
import { provideHttpClient } from '@angular/common/http';
import { provideHttpClientTesting } from '@angular/common/http/testing';
import { FormGroup, ReactiveFormsModule } from '@angular/forms';
import { provideRouter } from '@angular/router';
import { SharedModule } from '~/app/shared/shared.module';
import { SmbService } from '~/app/shared/api/smb.service';
import { USERSGROUPS_RESOURCE } from '../smb.model';
import { of } from 'rxjs';

export const FOO_USERSGROUPS = {
  users_groups_id: 'foo',
  values: {
    users: [
      {
        name: 'user',
        password: 'pass'
      }
    ],
    groups: [
      {
        name: 'bar'
      }
    ]
  },
  resource_type: USERSGROUPS_RESOURCE
};

describe('SmbUsersgroupsFormComponent', () => {
  let component: SmbUsersgroupsFormComponent;
  let fixture: ComponentFixture<SmbUsersgroupsFormComponent>;
  let createUsersGroups: jasmine.Spy;
  let getUsersGroups: jasmine.Spy;

  beforeEach(async () => {
    await TestBed.configureTestingModule({
      imports: [ToastrModule.forRoot(), SharedModule, ReactiveFormsModule],
      declarations: [SmbUsersgroupsFormComponent],
      providers: [provideHttpClient(), provideHttpClientTesting(), provideRouter([])]
    }).compileComponents();

    fixture = TestBed.createComponent(SmbUsersgroupsFormComponent);
    component = fixture.componentInstance;
    component.ngOnInit();
    createUsersGroups = spyOn(TestBed.inject(SmbService), 'createUsersGroups');
    getUsersGroups = spyOn(TestBed.inject(SmbService), 'getUsersGroups');
    fixture.detectChanges();
  });

  it('should create', () => {
    expect(component).toBeTruthy();
  });

  it('should set form invalid if required username is missing', () => {
    const user = component.users.controls[0] as FormGroup;
    component.form.controls['usersGroupsId'].setValue('foo');
    user.controls['name'].setValue('');
    expect(component.form.valid).not.toBeNull();
  });

  it('should set required fields, add group and submit the form', () => {
    const user = component.users.controls[0] as FormGroup;
    component.form.controls['usersGroupsId'].setValue('foo');
    component.form.controls['linkedToCluster'].setValue(undefined);
    user.controls['name'].setValue('user');
    user.controls['password'].setValue('pass');
    component.addGroup();
    const group = component.groups.controls[0] as FormGroup;
    group.controls['name'].setValue('bar');

    component.submit();

    expect(createUsersGroups).toHaveBeenCalledWith(FOO_USERSGROUPS);
  });

  describe('when editing', () => {
    beforeEach(() => {
      component.editing = true;
      getUsersGroups.and.returnValue(of(FOO_USERSGROUPS));
      component.ngOnInit();
      fixture.detectChanges();
    });

    it('should get resource data and set form fields with it', () => {
      expect(getUsersGroups).toHaveBeenCalled();
      expect(component.form.getRawValue()).toEqual({
        usersGroupsId: 'foo',
        users: [
          {
            name: 'user',
            password: 'pass'
          }
        ],
        groups: [
          {
            name: 'bar'
          }
        ],
        linkedToCluster: undefined
      });
    });
  });

  it('should add and remove users and groups', () => {
    const nUsers = component.users.length;
    const nGroups = component.groups.length;
    component.addUser();
    component.addGroup();
    component.addGroup();
    expect(component.users.length).toBe(nUsers + 1);
    expect(component.groups.length).toBe(nGroups + 2);
    component.removeUser(0);
    component.removeGroup(0);
    expect(component.users.length).toBe(nUsers);
    expect(component.groups.length).toBe(nGroups + 1);
  });
});
