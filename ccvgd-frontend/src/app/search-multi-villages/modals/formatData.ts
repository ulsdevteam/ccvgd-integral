export interface Category {
  category? : string
}

export interface CheckList {
  village_id: any,
  village_name: any,
  province: any,
  city: any,
  county: any,
  isSelected: boolean
}

export interface PostDataToSearch {
  villageid: any,
  topic:any,
  year?:any,
  year_range?:any
}


export interface Year {
  topic: string,
  village_id: any[]
  total_year_only?: any[],
  total_year_range?: any[]
}

export interface DisplayTopicCategory {
  selectedTopic: string,
  hasCategory: boolean,
  category1List?:any[],
  category2List?:any[],
  category3List?:any[]
}