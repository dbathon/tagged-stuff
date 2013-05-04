
module = angular.module('taggedStuff.directives', [])

module.directive 'appVersion', ['version', (version) ->

  (scope, elm, attrs) -> elm.text(version);

]


module.directive 'contentIf', ->
  transclude: true
  compile: (element, attrs, transclude) ->
    (scope, element, attrs) ->
      content = contentScope = null
      scope.$watch attrs.contentIf, (value) ->
        if content
          contentScope.$destroy()
          content.remove()
          content = null
        if value
          contentScope = scope.$new()
          transclude contentScope, (elem) ->
            content = elem
            element.append(content)


module.directive 'logDigest', ->
  (scope, element, attrs) ->
    name = attrs.logDigest || '???'
    scope.$watch ->
      console.log 'digest: ' + name
