import { AbstractControl, FormGroup } from '@angular/forms';

export const firstBy = (function() {
    /* mixin for the `thenBy` property */
    function extend(f) {
        f.thenBy = tb;
        return f;
    }
    /* adds a secondary compare function to the target function (`this` context)
       which is applied in case the first one returns 0 (equal)
       returns a new compare function, which has a `thenBy` method as well */
    function tb(y) {
        const x = this;
        return extend(function(a, b) {
            return x(a, b) || y(a, b);
        });
    }
    return extend;
  })();

function isEmptyInputValue(value: any): boolean {
  return !value || typeof value === 'string' && !value.trim();
}

export class CustomValidators {
  /**
   * Validator that requires controls to have a non-empty value (after trimming)
   */
  public static required(control: AbstractControl): { [key: string]: boolean } {
    return isEmptyInputValue(control.value) ? { 'required': true } : null;
  }

  /**
   * Validates whether form control value is a number
   * @param control
   */
  public static number(control: AbstractControl) {
    return isNaN(control.value) ? { 'number': true } : null;
  }
}

export const onValueChanged = (form: FormGroup, formErrors: {[key: string]: string },
                               formValidationMessages: object, data?: any) => {
  if (!form) { return; }
  const formCopy = form;

  for (const field in formErrors) {
    // clear previous error message (if any)
    formErrors[field] = '';
    const control = formCopy.get(field);
    if (control && control.dirty && !control.valid) {
      const messages = formValidationMessages[field];
      for (const key in control.errors) {
        formErrors[field] += messages[key] + ' ';
      }
    }
  }
};
