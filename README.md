# Druid exact distinct count aggregator

Provides a more reliable and efficient way to count the number of unique values in a column than existent approximate aggregators.

Nulls and empty strings are ignored by the aggregator. This means that they will not be counted as unique values.

To use the `exact-distinct-count-aggregator`, you can add it to your Druid native query as follows:

```
{
  "queryType": "timeseries",    
  "dataSource": {
    "type": "table",
    "name": "wikipedia"
  },
  "intervals": {
    "type": "intervals",
    "intervals": [
      "-146136543-09-08T08:23:32.096Z/146140482-04-24T15:36:27.903Z"
    ]
  },
  "filter": {
    "type": "not",
    "field": {
      "type": "expression",
      "expression": "isNew"
    }
  },
  "granularity": {
    "type": "all"
  },
  "aggregations": [
    {      
      "type": "exactDistinctCount", 
      "name": "test",                 // name to be displayed
      "fieldName": "comment",         // field to be counted
      "maxNumberOfValues": "5000"     // Integer parameter, default vlaue: 10000
    }
  ]
}
```

