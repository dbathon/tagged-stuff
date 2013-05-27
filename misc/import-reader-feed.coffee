http = require 'http'
https = require 'https'
fs = require 'fs'
url = require 'url'

toTag = (string) ->
  string = string.toLowerCase().replace /[^a-z0-9]+/g, '-'
  string.replace /^-|-$/g, ''

feedItemToEntry = (item, mainTag) ->
  entry =
    html: true
    reference: 'greader-' + item.id
    entryTs: item.crawlTimeMsec
    title: item.title
    url: item.alternate[0].href
    body: (item.content || item.summary || { content: null }).content
    tags: ['google-reader', mainTag]
  if item.origin.title
    entry.tags.push toTag(item.origin.title)
  entry

postEntry = (postUrl, entry, callback) ->
  options = url.parse postUrl
  options.method = 'POST'
  options.headers =
    'Content-Type': 'application/json'

  prot = if options.protocol == 'https:' then https else http

  req = prot.request options, (res) ->
    res.on 'data', ->
    res.on 'end', ->
      callback res.statusCode, entry

  req.write JSON.stringify entry
  req.end()


file = fs.readFileSync process.argv[2]
feed = JSON.parse file

mainTag = process.argv[3]
postUrl = process.argv[4]

postItems = (items) ->
  if items.length > 0
    item = items.shift()
    postEntry postUrl, feedItemToEntry(item, mainTag), (status, entry) ->
      console.log 'status: ' + status + ', reference: ' + entry.reference
      postItems items

postItems feed.items
