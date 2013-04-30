
module = angular.module 'taggedStuff.controllers', []

module.controller 'MainCtrl', ['$scope', (s) ->
  console.log 'MainCtrl'
  console.log s

  s.tags = [
    { name: 'foo' }
    { name: 'bar' }
    { name: 'baz' }
    { name: 'more' }
  ]
]

module.controller 'MyCtrl2', [() ->

  console.log 'MyCtrl2'
]
