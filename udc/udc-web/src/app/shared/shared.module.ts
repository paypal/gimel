import { CommonModule } from '@angular/common';
import { NgModule } from '@angular/core';
import { FormsModule } from '@angular/forms';
import { MaterialModule } from '@angular/material';
import { HighlightPipe } from './pipes/highlight.pipe';
import { RxContextDirective } from './directives/rx-context.directive';

@NgModule({
  imports: [
    MaterialModule,
    CommonModule,
    FormsModule,
  ],
  declarations: [
    RxContextDirective,
    HighlightPipe,
  ],
  exports: [
    HighlightPipe,
    RxContextDirective,
    CommonModule,
    FormsModule,
  ],
})
export class SharedModule { }
