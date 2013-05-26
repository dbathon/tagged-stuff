
module = angular.module 'taggedStuff.filters', []

module.filter 'interpolate', ['version', (version) ->
  (text) ->
    String(text).replace(/\%VERSION\%/mg, version)
]

module.filter 'timestamp', ['$filter', ($filter) ->
  dateFilter = $filter 'date'
  (ts) ->
    dateFilter ts, 'yyyy-MM-dd HH:mm:ss'
]
