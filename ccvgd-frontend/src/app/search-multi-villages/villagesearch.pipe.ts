
import { Pipe, PipeTransform } from '@angular/core';

@Pipe({
  name: 'villagesearch'
})
export class VillagesearchPipe implements PipeTransform {

  transform(items: Array<any>, provinceSearch: string, citySearch: string, countySearch: string, villageSearch: string){

    // console.log("items", items);
    // // console.log(provinceSearch);

    if(items && items!= undefined) {
      console.log(typeof items);
      return Object.values(items).filter(item => {
        // console.log(provinceName.city);
        const villageName = item.name;
        const province = item.province;
        const city = item.city;
        const county = item.county;

        console.log(`${villageName} ${province} ${city} ${county}`);
        console.log(province);
        if(provinceSearch && province.indexOf(provinceSearch) === -1) {
          return false;
        }
        if(citySearch && city.indexOf(citySearch) === -1) {
          return false;
        }
        if(countySearch && county.indexOf(countySearch) === -1) {
          return false;
        }
        if(villageSearch && villageName.indexOf(villageSearch) === -1) {
          return false;
        }

        return true;
      })
    }
    else{
      console.log("no input");
      return items;
    }

  }
}
