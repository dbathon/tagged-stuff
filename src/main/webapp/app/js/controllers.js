// Generated by CoffeeScript 1.6.1
(function() {
  var module;

  module = angular.module('taggedStuff.controllers', []);

  module.controller('SearchCtrl', [
    '$scope', 'searchService', function(s, searchService) {
      s.data = {
        searchString: ''
      };
      s.search = function() {
        return searchService.search(s.data.searchString);
      };
      s.searchAll = function() {
        return searchService.search(null);
      };
      return searchService.addListener(s, function(searchString) {
        return s.data.searchString = searchString;
      });
    }
  ]);

  module.controller('MainCtrl', [
    '$scope', 'entryService', 'tagService', 'searchService', function(s, entryService, tagService, searchService) {
      var updateEntries, updateTags;
      s.data = {
        searchString: null
      };
      updateTags = function() {
        return s.tags = tagService.query({
          orderBy: 'id'
        });
      };
      updateEntries = function() {
        return s.entries = entryService.query({
          orderBy: '-createdTs',
          query: s.data.searchString
        });
      };
      s.searchForTag = function(tag) {
        return searchService.search('+' + tag.id);
      };
      s.entriesTitle = function() {
        if (s.data.searchString) {
          return 'Search result for "' + s.data.searchString + '"';
        } else {
          return 'All entries';
        }
      };
      searchService.addListener(s, function(searchString) {
        s.data.searchString = searchString && searchString.length > 0 ? searchString : null;
        return updateEntries();
      });
      updateTags();
      return updateEntries();
    }
  ]);

  module.controller('MyCtrl2', [function() {}]);

}).call(this);
