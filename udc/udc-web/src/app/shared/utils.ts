/*
 * Copyright 2019 PayPal Inc.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
