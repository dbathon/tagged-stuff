'use strict';

// Declare app level module which depends on filters, and services
angular.module('taggedStuff',
    ['taggedStuff.filters', 'taggedStuff.services', 'taggedStuff.directives', 'taggedStuff.controllers']).config(
    ['$routeProvider', function($routeProvider) {
      $routeProvider.when('/', {
        templateUrl : 'partials/main.html',
        controller : 'MainCtrl'
      });
      $routeProvider.when('/view2', {
        templateUrl : 'partials/partial2.html',
        controller : 'MyCtrl2'
      });
      $routeProvider.otherwise({
        redirectTo : '/'
      });
    }]);
