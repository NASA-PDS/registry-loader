#!/bin/bash

update_schema() 
{
curl -k -X PUT "$elastic_url/registry/_mapping" -H 'Content-Type: application/json' -d'
{
  "properties": {
    "ops:Tracking_Meta/ops:archive_status": { "type": "keyword" }
  }
}
'
}

set_archive_status()
{
curl -k -X POST "$elastic_url/registry/_update_by_query" -H 'Content-Type: application/json' -d'
{
  "script" : {
    "source": "ctx._source['params.field'] = params.value",
    "params": {
      "field": "ops:Tracking_Meta/ops:archive_status", 
      "value": "staged"
    }
  },
  "query": { "match_all": {} }
}
'
}


if [[ $# -eq 1 ]]; then
    elastic_url=$1
    update_schema
    set_archive_status
else
    echo "Usage: $(basename $BASH_SOURCE) <OpenSearch URL> " >&2
    exit 2
fi
