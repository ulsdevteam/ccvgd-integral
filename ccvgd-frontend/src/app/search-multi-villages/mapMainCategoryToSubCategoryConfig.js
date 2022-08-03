const mapMainCategoryToSubCategoryConfig = {
    "自然环境": {data: [], target:"村庄基本信息"},
    "村志基本信息": {data: [], target:"村庄基本信息"},
    "自然灾害":{data: [], target:"村庄基本信息"},
    "姓氏": {data: [], target:"村庄基本信息"},
    "第一次拥有或购买年份": {data: [], target:"村庄基本信息"},
    "民族": {data: [], target:"村庄基本信息"},
    "计划生育": {data: [], target:"人口"},
    "教育":  {data: [], target:"村庄基本信息"},
};
const middleTabsMap = new Map([
    ["村庄基本信息", "village"],
    ["村志基本信息", "gazetteerinformation"],
    ["自然环境", "naturalenvironment"],
    ["自然灾害", "naturaldisasters"],
    ["姓氏", "fourthlastNames"],
    ["第一次拥有或购买年份", "firstavailabilityorpurchase"],
    ["民族", "ethnicgroups"],
    ["人口", "population"],
    ["军事政治", "military"],
    ["经济原始数据", "economy"],
    ["统一单位经济", "economyunity"],
    ["计划生育", "familyplanning"],
    ["教育", "education"]
]);
export default {middleTabsMap, mapMainCategoryToSubCategoryConfig};