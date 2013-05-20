
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
        entity
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
