
module = angular.module 'taggedStuff.controllers', []

module.controller 'SearchCtrl', ['$scope', 'searchService', (s, searchService) ->
  s.data =
    searchString: ''

  s.search = -> searchService.search s.data.searchString
  s.searchAll = -> searchService.search null

  searchService.addListener s, (searchString) ->
    s.data.searchString = searchString
]

module.controller 'MainCtrl', ['$scope', 'entryService', 'tagService', 'searchService', (s, entryService, tagService, searchService) ->
  s.data =
    searchString: null

  updateTags = ->
    s.tags = tagService.query { orderBy: 'id' }

  updateEntries = ->
    s.entries = entryService.query { orderBy: '-createdTs', query: s.data.searchString }

  s.searchForTag = (tag) ->
    searchService.search '+' + tag.id

  s.entriesTitle = ->
    if s.data.searchString
      'Search result for "' + s.data.searchString + '"'
    else
      'All entries'

  searchService.addListener s, (searchString) ->
    s.data.searchString = if searchString && searchString.length > 0 then searchString else null
    updateEntries()

  updateTags()
  updateEntries()
]

module.controller 'MyCtrl2', [() ->

]
