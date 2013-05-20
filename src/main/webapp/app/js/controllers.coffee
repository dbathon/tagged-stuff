
module = angular.module 'taggedStuff.controllers', []

module.controller 'RootCtrl', ['$scope', '$rootScope', (s, $rootScope) ->
  eventFromInput = (event) ->
    angular.lowercase(event.target.nodeName) in ['input', 'textarea']

  s.broadcastKeypress = (event) ->
    if !eventFromInput(event)
      $rootScope.$broadcast('global.keypress', event)

  s.broadcastKeyup = (event) ->
    if !eventFromInput(event)
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

  s.tags = []

  updateTags = ->
    tagService.query { orderBy: 'id' }, s.tags

  s.searchForTag = (tag) ->
    searchService.search '+' + tag.id

  s.$on 'entry.saved', ->
    updateTags()

  updateTags()
]

module.controller 'EntriesCtrl', ['$scope', 'entryService', 'searchService', (s, entryService, searchService) ->
  s.data =
    searchString: null

  selectedIndex = null

  s.entries = []

  updateEntries = ->
    selectedIndex = null
    entryService.query { orderBy: '-createdTs', query: s.data.searchString }, s.entries

  s.entriesTitle = ->
    if s.data.searchString
      'Search result for "' + s.data.searchString + '"'
    else
      'All entries'

  s.isSelected = (entry) ->
    entry == s.entries[selectedIndex]

  s.getSelectedEntry = ->
    if selectedIndex?
      s.entries[selectedIndex]
    else
      null

  s.isExpanded = (entry) ->
    s.isSelected entry

  s.select = (entry) ->
    index = s.entries.indexOf(entry)
    if selectedIndex == index && !s.isCurrentEntryNew()
      # unselect
      selectedIndex = null
    else
      selectedIndex = if index >= 0 then index else null

  s.down = ->
    if s.entries.length > 0
      if selectedIndex? && selectedIndex < s.entries.length - 1
        ++selectedIndex
      else if !selectedIndex?
        selectedIndex = 0

  s.up = ->
    if s.entries.length > 0
      if selectedIndex > 0
        --selectedIndex
      else if !selectedIndex?
        selectedIndex = s.entries.length - 1

  s.newEntry = ->
    if !s.isCurrentEntryNew()
      s.entries.unshift { tags: [] }
      selectedIndex = 0

  s.isCurrentEntryNew = ->
    selectedIndex == 0 && !s.entries[0].id

  s.cancelNewEntry = ->
    if s.isCurrentEntryNew()
      s.entries.shift()

  s.$watch 'isCurrentEntryNew()', (newValue, oldValue) ->
    # cancel new entry when the selection changes
    if oldValue && !newValue && !s.entries[0].id
      oldSelectedIndex = selectedIndex
      # temporarily select 0 again for cancel
      selectedIndex = 0
      s.cancelNewEntry()
      # restore and "fix" selectedIndex
      selectedIndex = if oldSelectedIndex then oldSelectedIndex - 1 else oldSelectedIndex

  s.joinedTags = (entry) ->
    (tag.id for tag in entry.tags).sort().join ' '

  searchService.addListener s, (searchString) ->
    s.data.searchString = if searchString && searchString.length > 0 then searchString else null
    updateEntries()

  s.$on 'global.keypress', (_, event) ->
    switch event.keyCode || event.charCode
      when 106 # j
        s.down()
        event.preventDefault()
      when 107 # k
        s.up()
        event.preventDefault()
      when 110 # n
        s.newEntry()
        event.preventDefault()

  updateEntries()
]

module.controller 'EntryCtrl', ['$scope', 'entryService', '$window', '$rootScope', (s, entryService, $window, $rootScope) ->
  s.bodyLines = (entry) ->
    if entry.body
      line.trim() for line in entry.body.split '\n'  when line.trim().length > 0
    else
      []

  s.sortedTags = (entry) ->
    (tag.id for tag in entry.tags).sort()

  s.editing = false
  s.edited = {}
  s.data = { tagsText: null }

  s.startEdit = (entry) ->
    if !s.editing
      angular.copy entry, s.edited
      s.data.tagsText = s.sortedTags(entry).join ' '
      s.editing = true

  s.isEditing = (entry) ->
    if s.editing
      true
    else
      # automatically start editing if it is a new entry
      if !entry.id
        s.startEdit(entry)
      s.editing

  s.saveEdit = (entry) ->
    if s.editing
      # make a backup in case the save fails
      entryBackup = angular.copy entry
      # apply changes
      angular.copy s.edited, entry
      # process tags
      entry.tags = ({ id: tag.trim() } for tag in s.data.tagsText.split ' '  when tag.trim().length > 0)

      promise = entryService.save entry
      promise.success ->
        s.editing = false
        $rootScope.$broadcast 'entry.saved'
      promise.error (data, status) ->
        angular.copy entryBackup, entry
        $window.alert 'Save failed: ' + (if angular.isObject(data) && data.error then data.error else 'Status: ' + status)

  s.cancelEdit = (entry) ->
    if s.editing
      s.editing = false
      # see EntriesCtrl...
      s.cancelNewEntry()

  s.$on 'global.keypress', (_, event) ->
    switch event.keyCode || event.charCode
      when 101 # e
        entry = s.getSelectedEntry()
        if entry
          s.startEdit entry
          event.preventDefault()

  s.formKeydown = (event) ->
    switch event.keyCode
      when 27 # ESC
        entry = s.getSelectedEntry()
        if entry
          s.cancelEdit entry
          event.preventDefault()
]

