import { TestBed } from '@angular/core/testing';
import { StorageClassService } from './storage-class.service';

describe('StorageClassService', () => {
  let service: StorageClassService;

  beforeEach(() => {
    TestBed.configureTestingModule({
      providers: [StorageClassService]
    });
    service = TestBed.inject(StorageClassService);
  });

  it('should be created', () => {
    expect(service).toBeDefined();
  });

  describe('storageClassForm setter', () => {
    it('should set the storage class form', () => {
      const form = {
        storage_class: 'test-class',
        zonegroup: 'default'
      };

      service.storageClassForm = form;
      expect(service.storageClassForm).toEqual(form);
    });

    it('should replace existing form when setting a new one', () => {
      const form1 = {
        storage_class: 'class-1'
      };

      const form2 = {
        storage_class: 'class-2'
      };

      service.storageClassForm = form1;
      expect(service.storageClassForm).toEqual(form1);

      service.storageClassForm = form2;
      expect(service.storageClassForm).toEqual(form2);
      expect(service.storageClassForm === form1).toBeFalsy();
    });
  });

  describe('storageClassForm getter', () => {
    it('should return undefined when no form is set', () => {
      expect(service.storageClassForm).toBeUndefined();
    });

    it('should return the stored form after setting it', () => {
      const form = {
        storage_class: 'test-class',
        region: 'us-east-1'
      };

      service.storageClassForm = form;
      const retrievedForm = service.storageClassForm;

      expect(retrievedForm).toEqual(form);
      expect(retrievedForm.storage_class).toEqual('test-class');
      expect(retrievedForm.region).toEqual('us-east-1');
    });

    it('should return a deep clone of the stored form values', () => {
      const form = {
        storage_class: 'test-class',
        acls: [{ source_id: 'test@example.com', dest_id: 'backup@example.com', type: 'email' }]
      };
      service.storageClassForm = form;
      const retrievedForm = service.storageClassForm;

      retrievedForm.acls[0].source_id = 'updated@example.com';
      expect(service.storageClassForm?.acls[0].source_id).toEqual('test@example.com');
    });
  });

  describe('clearStorageClassForm', () => {
    it('should clear the stored form', () => {
      const form = {
        storage_class: 'test-class'
      };

      service.storageClassForm = form;
      expect(service.storageClassForm).toEqual(form);

      service.clearStorageClassForm();
      expect(service.storageClassForm).toBeNull();
    });

    it('should not throw error when clearing already empty form', () => {
      const fn = () => service.clearStorageClassForm();
      expect(fn).not.toThrow();
      expect(service.storageClassForm).toBeNull();
    });
  });

  describe('hasStorageClassForm', () => {
    it('should return false when no form is set', () => {
      expect(service.hasStorageClassForm()).toBeFalsy();
    });

    it('should return true when form is set', () => {
      const form = {
        storage_class: 'test-class'
      };

      service.storageClassForm = form;
      expect(service.hasStorageClassForm()).toBeTruthy();
    });

    it('should return false after clearing the form', () => {
      const form = {
        storage_class: 'test-class'
      };

      service.storageClassForm = form;
      expect(service.hasStorageClassForm()).toBeTruthy();

      service.clearStorageClassForm();
      expect(service.hasStorageClassForm()).toBeFalsy();
    });
  });

  describe('service lifecycle', () => {
    it('should handle multiple set and clear operations', () => {
      const form1 = {
        storage_class: 'class-1'
      };

      const form2 = {
        storage_class: 'class-2'
      };

      // Set form1
      service.storageClassForm = form1;
      expect(service.hasStorageClassForm()).toBeTruthy();
      expect(service.storageClassForm).toEqual(form1);

      // Replace with form2
      service.storageClassForm = form2;
      expect(service.hasStorageClassForm()).toBeTruthy();
      expect(service.storageClassForm).toEqual(form2);

      // Clear
      service.clearStorageClassForm();
      expect(service.hasStorageClassForm()).toBeFalsy();
      expect(service.storageClassForm).toBeNull();

      // Set form1 again
      service.storageClassForm = form1;
      expect(service.hasStorageClassForm()).toBeTruthy();
      expect(service.storageClassForm).toEqual(form1);
    });
  });
});
