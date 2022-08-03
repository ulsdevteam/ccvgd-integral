import { Injectable } from '@angular/core';

@Injectable({
  providedIn: 'root'
})
export class ProvinceCityCountyService {

  constructor() { }

  getAll(): any{
    return [
      {
        province:"河南省",
        cityList:[
          {
            city: "河南省市1",
            county:["河南省市1县1","河南省市1县2","河南省市1县3"]
          },
          {
            city: "河南省市2",
            county:["河南省市2县1","河南省市2县2","河南省市2县3"]
          },
       ],
      },
      {
        province:"河北省",
        cityList:[
          {
            city: "河北省市1",
            county:["河北省市1县1","河北省市1县2","河北省市1县3"]
          },
          {
            city: "河北省市2",
            county:["河北省市2县1","河北省市2县2","河北省市2县3"]
          },
        ],
      },
    ]
  }



  getProvince(): string[]{
    return ["河北省","山西省","辽宁省","吉林省","黑龙江省","江苏省","浙江省","安徽省",
            "福建省","江西省","山东省","河南省","湖北省","湖南省","广东省","海南省",
            "四川省","贵州省","云南省","陕西省","甘肃省","青海省","台湾省"];
  }
}
