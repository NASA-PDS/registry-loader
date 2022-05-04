#!/bin/bash

update_schema() 
{
curl -k $elastic_auth -X PUT "$elastic_url/registry/_mapping" -H 'Content-Type: application/json' -d'
{
  "properties": {
    "ops:Tracking_Meta/ops:archive_status": { "type": "keyword" }
  }
}
'
}

set_archive_status()
{
curl -k $elastic_auth -X POST "$elastic_url/registry/_update_by_query" -H 'Content-Type: application/json' -d'
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
elif [[ $# -eq 3 ]]; then
    elastic_url=$1
    elastic_auth="-u $2:$3"
    update_schema
    set_archive_status
else
    echo "Usage: $(basename $BASH_SOURCE) <OpenSearch URL> [<username> <password>]" >&2
    exit 2
fi
