
module = angular.module 'taggedStuff.controllers', []

module.controller 'RootCtrl', ['$scope', '$rootScope', (s, $rootScope) ->
  s.broadcastKeypress = (event) ->
    console.log event
    console.log 'press-' + (event.keyCode || event.charCode) + '-' + angular.lowercase(event.target.nodeName)
    console.log (k for own k, v of event when v && k.match(/Key$/)).sort().join '-'
    $rootScope.$broadcast('global.keypress', event)

  s.broadcastKeyup = (event) ->
    console.log event
    console.log 'up-' + event.keyCode + '-' + angular.lowercase(event.target.nodeName)
    console.log (k for own k, v of event when v && k.match(/Key$/)).sort().join '-'
    $rootScope.$broadcast('global.keyup', event)
]

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

  s.$on 'global.keypress', (_, event) ->
    return if angular.lowercase(event.target.nodeName) == 'input'
    switch event.keyCode || event.charCode
      when 106
        # j -> down
        if s.entries.length > 0
          if selectedIndex? && selectedIndex < s.entries.length - 1
            ++selectedIndex
          else if !selectedIndex?
            selectedIndex = 0
      when 107
        # k -> up
        if s.entries.length > 0
          if selectedIndex > 0
            --selectedIndex
          else if !selectedIndex?
            selectedIndex = s.entries.length - 1

  updateEntries()
]

