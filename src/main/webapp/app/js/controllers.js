// Generated by CoffeeScript 1.6.1
(function() {
  var module;

  module = angular.module('taggedStuff.controllers', []);

  module.controller('MainCtrl', [
    '$scope', function(s) {
      console.log('MainCtrl');
      console.log(s);
      return s.tags = [
        {
          name: 'foo'
        }, {
          name: 'bar'
        }, {
          name: 'baz'
        }, {
          name: 'more'
        }
      ];
    }
  ]);

  module.controller('MyCtrl2', [
    function() {
      return console.log('MyCtrl2');
    }
  ]);

}).call(this);
