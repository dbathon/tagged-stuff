// Generated by CoffeeScript 1.6.1
(function() {
  var module, type, _fn, _i, _len, _ref;

  module = angular.module('taggedStuff.directives', []);

  module.directive('appVersion', [
    'version', function(version) {
      return function(scope, elm, attrs) {
        return elm.text(version);
      };
    }
  ]);

  module.directive('navItem', function() {
    return {
      restrict: 'E',
      transclude: true,
      scope: {
        location: '@location'
      },
      controller: [
        '$scope', '$location', function(s, $location) {
          return s.isActive = function() {
            return s.location === $location.path();
          };
        }
      ],
      template: '<li ng-class="{active: isActive()}"><a ng-href="#{{location}}" ng-transclude></a></li>',
      replace: true
    };
  });

  module.directive('contentIf', function() {
    return {
      transclude: true,
      compile: function(element, attrs, transclude) {
        return function(scope, element, attrs) {
          var content, contentScope;
          content = contentScope = null;
          return scope.$watch(attrs.contentIf, function(value) {
            if (content) {
              contentScope.$destroy();
              content.remove();
              content = null;
            }
            if (value) {
              contentScope = scope.$new();
              return transclude(contentScope, function(elem) {
                content = elem;
                return element.append(content);
              });
            }
          });
        };
      }
    };
  });

  module.directive('logDigest', function() {
    return function(scope, element, attrs) {
      var name;
      name = attrs.logDigest || '???';
      return scope.$watch(function() {
        return console.log('digest: ' + name);
      });
    };
  });

  _ref = ['press', 'down', 'up'];
  _fn = function(type) {
    return module.directive('onKey' + type, [
      '$parse', function($parse) {
        return function(scope, element, attrs) {
          var fn;
          fn = $parse(attrs['onKey' + type]);
          return element.bind('key' + type, function(event) {
            return scope.$apply(function() {
              return fn(scope, {
                $event: event
              });
            });
          });
        };
      }
    ]);
  };
  for (_i = 0, _len = _ref.length; _i < _len; _i++) {
    type = _ref[_i];
    _fn(type);
  }

}).call(this);
