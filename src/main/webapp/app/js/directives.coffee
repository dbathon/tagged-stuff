
module = angular.module('taggedStuff.directives', [])

module.directive 'appVersion', ['version', (version) ->
  (scope, elm, attrs) -> elm.text(version);
]


module.directive 'navItem', ->
  restrict: 'E'
  transclude: true
  scope:
    location: '@location'
  controller: ['$scope', '$location', (s, $location) ->
    s.isActive = ->
      s.location == $location.path()
  ]
  template: '<li ng-class="{active: isActive()}"><a ng-href="#{{location}}" ng-transclude></a></li>'
  replace: true


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


for type in ['press', 'down', 'up']
  do (type) ->
    module.directive 'onKey' + type, ['$parse', ($parse) ->
      (scope, element, attrs) ->
        fn = $parse attrs['onKey' + type]
        element.bind 'key' + type, (event) ->
          scope.$apply ->
            fn scope, { $event: event }
    ]
