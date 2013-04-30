
# Declare app level module which depends on filters, and services
app = angular.module 'taggedStuff',
  ['taggedStuff.filters', 'taggedStuff.services', 'taggedStuff.directives', 'taggedStuff.controllers']

app.config ['$routeProvider', ($routeProvider) ->

  $routeProvider.when '/',
    templateUrl : 'partials/main.html',
    controller : 'MainCtrl'

  $routeProvider.when '/view2',
    templateUrl : 'partials/partial2.html',
    controller : 'MyCtrl2'

  $routeProvider.otherwise
    redirectTo : '/'

]
