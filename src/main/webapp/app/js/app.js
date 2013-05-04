// Generated by CoffeeScript 1.6.1
(function() {
  var app;

  app = angular.module('taggedStuff', ['taggedStuff.filters', 'taggedStuff.services', 'taggedStuff.directives', 'taggedStuff.controllers']);

  app.config([
    '$routeProvider', function($routeProvider) {
      $routeProvider.when('/', {
        templateUrl: 'partials/main.html',
        controller: 'MainCtrl'
      });
      $routeProvider.when('/view2', {
        templateUrl: 'partials/partial2.html',
        controller: 'MyCtrl2'
      });
      return $routeProvider.otherwise({
        redirectTo: '/'
      });
    }
  ]);

  app.run([
    '$injector', function($injector) {
      return window.ngInjector = $injector;
    }
  ]);

}).call(this);
