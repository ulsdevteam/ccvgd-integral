import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class FilterServiceService {

  constructor() { }

  //后端返回的filter:{} 结构或者filter_2:{} 结构
  filterItem(treeFilter: any): any {
    if(treeFilter === undefined){
      return {};
    }

  return treeFilter[0];
  }



}
