
> https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-index_.html
> https://www.elastic.co/guide/en/elasticsearch/reference/7.17/docs-bulk.html
> https://elasticsearch.bookhub.tech/rest_apis/document_apis/reindex
 
## 1 _doc
> PUT is same as POST    

| No. | action  | url                             | id            | remarks                                        |     |
| --- | ---     | ---                             | ---           | ---                                            | --- |
| 1   | _doc    |  POST /{index}/_doc/            | -             | create doc with generated id                   |     |
| 2   | _doc    |  POST /{index}/_doc/{id}        | not exist     | create doc with specified id                   |     |
| 3   | _doc    |  POST /{index}/_doc/{id}        | exist         | overwrite doc with specified id                |     |
| 4   | _doc    |  PUT  /{index}/_doc/{id}        | not exist     | create doc with specified id                   |     |
| 5   | _doc    |  PUT  /{index}/_doc/{id}        | exist         | overwrite doc with specified id                |     |


## 2 _create
> PUT is same as POST    

| No. | action  | url                             | id            | remarks                                        |     |
| --- | ---     | ---                             | ---           | ---                                            | --- |
| 1   | _create | POST /{index}/_create/{id}      | not exist     | create doc with specified id                   |     |
| 2   | _create | ~~POST /{index}/_create/{id}~~  | ~~exist~~     | ~~version conflict, document already exists~~  |     |
| 3   | _create | PUT   /{index}/_create/{id}     | not exist     | create doc with specified id                   |     |
| 4   | _create | ~~PUT   /{index}/_create/{id}~~ | ~~exist~~     | ~~version conflict, document already exists~~  |     |


## 3 delete
| No. | action  | url                             | id            | remarks                                        |     |
| --- | ---     | ---                             | ---           | ---                                            | --- |
| 1   | _doc    | DELETE /{index}/_doc/{id}       | not exist     | delete doc with specified id                   |     |


## 4 bulk
> https://www.elastic.co/guide/en/elasticsearch/reference/current/docs-bulk.html    
``` json
// POST /{index}/_bulk
{ "index" : { "_index" : "test", "_id" : "1" } }
{ "field1" : "value1" }
{ "delete" : { "_index" : "test", "_id" : "2" } }
{ "create" : { "_index" : "test", "_id" : "3" } }
{ "field1" : "value3" }
{ "update" : {"_id" : "1", "_index" : "test"} }
{ "doc" : {"field2" : "value2"} }
```