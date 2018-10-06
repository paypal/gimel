import {Injectable} from '@angular/core';
import {MdSnackBarConfig} from '@angular/material';

@Injectable()
export class ConfigService {
  userName: string;
  snackBarConfig = new MdSnackBarConfig();
  apiConfig: { [key: string]: string };

  constructor() {
    this.snackBarConfig.duration = 15000;
  }


  public get(key: string): string {
    return this.apiConfig[key];
  }

}
