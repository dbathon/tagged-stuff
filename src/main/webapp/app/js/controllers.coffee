
module = angular.module 'taggedStuff.controllers', []

module.controller 'SearchCtrl', ['$scope', 'searchService', (s, searchService) ->
  s.data =
    searchString: ''

  s.search = -> searchService.search s.data.searchString
  s.searchAll = -> searchService.search null

  searchService.addListener s, (searchString) ->
    s.data.searchString = searchString
]

module.controller 'TagsCtrl', ['$scope', 'tagService', 'searchService', (s, tagService, searchService) ->
  s.data =
    searchString: null

  updateTags = ->
    s.tags = tagService.query { orderBy: 'id' }

  s.searchForTag = (tag) ->
    searchService.search '+' + tag.id

  updateTags()
]

module.controller 'EntriesCtrl', ['$scope', 'entryService', 'searchService', (s, entryService, searchService) ->
  s.data =
    searchString: null

  updateEntries = ->
    s.entries = entryService.query { orderBy: '-createdTs', query: s.data.searchString }

  s.entriesTitle = ->
    if s.data.searchString
      'Search result for "' + s.data.searchString + '"'
    else
      'All entries'

  searchService.addListener s, (searchString) ->
    s.data.searchString = if searchString && searchString.length > 0 then searchString else null
    updateEntries()

  updateEntries()
]

module.controller 'MyCtrl2', [() ->

]
