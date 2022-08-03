import { Injectable } from '@angular/core';
import {TableData} from './village-name.service';

@Injectable({
  providedIn: 'root'
})
export class StateServiceService {

  data: TableData[];

  constructor() { }
}
