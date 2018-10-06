import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'highlight',
})
export class HighlightPipe implements PipeTransform {
  transform(text: string, query: string): string {
    return text ? text.replace(query, `<span class="highlight">${ query }</span>`) : text;
  }
}
