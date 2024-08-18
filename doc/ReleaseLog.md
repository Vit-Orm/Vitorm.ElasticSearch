# Vitorm.ElasticSearch ReleaseLog

-----------------------
# 2.0.5
- refactor QueryExecutor

-----------------------
# 2.0.3
- support BatchQuery by ElasticSearch scroll api
- support BatchAsync

-----------------------
# 2.0.2
- support Convert
- support is null and is not null
- support Property<T>("path.path2")
- fix Result window too large issue(from + size must be less than or equal to: [10000]).
- FilterRuleQueryBuilder support IsNull IsNotNull ==null !=null
- support Date format
- support add new Entity without ElasticSearch  key


-----------------------
# 2.0.1

- new method DbContext.GetMappingAsync and DbContext.GetMapping
- support ToListAsync in Query
- fix string keyword query issue
- support String.Like and String.Match
- support query with nested documents by NestedField_Extensions.Who


-----------------------
# 2.0.0

 - Extension Method to get Result with TotalCount and score


