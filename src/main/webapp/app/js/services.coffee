
module = angular.module 'taggedStuff.services', []

# Demonstrate how to register services
# In this case it is a simple value service.
module.value 'version', '0.2'

l = (o) -> console.log o

module.factory 'baseRestPath', ['$window', ($window) ->
  path = $window.location.pathname
  (path.substring 0, path.lastIndexOf '/app') + '/rest/'
]

module.factory 'entityServiceFactory', ['$http', 'baseRestPath', ($http, baseRestPath) ->
  (entityName) ->
    basePath = baseRestPath + 'entity/' + entityName + '/'

    pathWithId = (id) ->
      basePath + id

    errorLogger = (data, status, headers, config) ->
      # TODO: make this better and configurable
      l [data, status, headers, config]

    {
      query: (params, targetArray) ->
        promise = $http { method: 'GET', url: basePath, params: params }
        if angular.isArray targetArray
          promise.success (data) ->
            targetArray.length = 0
            targetArray.push data.result...
        promise.error errorLogger
        promise

      save: (entity) ->
        request =
          data: entity
          headers:
            'Content-Type': 'application/json'

        if entity.id
          # existing -> put an update
          request.method = 'PUT'
          request.url = pathWithId(entity.id)
        else
          # new entity -> post
          request.method = 'POST'
          request.url = basePath

        promise = $http request
        promise.success (data) ->
          angular.copy data, entity
        promise.error errorLogger
        promise
    }
]

module.factory 'tagService', ['entityServiceFactory', (entityServiceFactory) ->
  entityServiceFactory 'tag'
]

module.factory 'entryService', ['entityServiceFactory', (entityServiceFactory) ->
  entityServiceFactory 'entry'
]

module.factory 'searchService', ['$rootScope', ($rootScope) ->
  search: (searchString) ->
    $rootScope.$broadcast 'searchService.executeSearch', searchString

  addListener: (scope, callback) ->
    scope.$on 'searchService.executeSearch', (event, searchString) ->
      callback searchString
]

module.factory 'focus', ->
  focus =
    focusId: null

    requestFocus: (id) ->
      focus.focusId = id

    reset: ->
      focus.focusId = null
