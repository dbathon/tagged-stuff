
module = angular.module 'taggedStuff.filters', []

module.filter 'interpolate', ['version', (version) ->

  (text) -> String(text).replace(/\%VERSION\%/mg, version);

]
