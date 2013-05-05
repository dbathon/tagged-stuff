
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

  selectedIndex = null

  updateEntries = ->
    selectedIndex = null
    s.entries = entryService.query { orderBy: '-createdTs', query: s.data.searchString }

  s.entriesTitle = ->
    if s.data.searchString
      'Search result for "' + s.data.searchString + '"'
    else
      'All entries'

  s.isSelected = (entry) ->
    entry == s.entries[selectedIndex]

  s.isExpanded = (entry) ->
    s.isSelected entry

  s.select = (entry) ->
    index = s.entries.indexOf(entry)
    selectedIndex = if index >= 0 then index else null

  s.joinedTags = (entry) ->
    (tag.id for tag in entry.tags).sort().join ' '

  searchService.addListener s, (searchString) ->
    s.data.searchString = if searchString && searchString.length > 0 then searchString else null
    updateEntries()

  updateEntries()
]

