
module = angular.module('taggedStuff.directives', [])

module.directive 'appVersion', ['version', (version) ->

  (scope, elm, attrs) -> elm.text(version);

]
