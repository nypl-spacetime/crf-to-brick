#!/usr/bin/env node

const fs = require('fs')
const path = require('path')
const H = require('highland')
const brickDb = require('to-brick')
const argv = require('minimist')(process.argv.slice(2))

const ORGANIZATION_ID = 'nypl'
const DATA_DIR = path.join(__dirname, 'data')
const COLLECTIONS = require(path.join(DATA_DIR, 'collections.json'))

var cityDirectoriesDir
if (argv._ && argv._.length) {
  cityDirectoriesDir = argv._[0]
} else {
  console.error('Please specify City Directory data directory')
  process.exit(1)
}

const TASKS = [
  {
    id: 'label-fields',
    submissionsNeeded: 5
  }
]

const tasks = TASKS
  .map((task) => ({
    id: task.id
  }))

const collections = COLLECTIONS
  .map((collection) => ({
    organization_id: ORGANIZATION_ID,
    tasks: TASKS,
    id: collection.uuid,
    url: collection.url,
    data: {
      fields: collection.fields
    }
  }))

function createLinesStream(collection) {
  // Now uses every tenth line
  // Maybe use every first 25 of each page instead?
  var i = 0
  var lastPageNum = 0
  return H(fs.createReadStream(collection.lines, 'utf8'))
    .split()
    .compact()
    .map(JSON.parse)
    .map((line) => {
      const pageNum = line.page_num
      if (lastPageNum !== pageNum) {
        i = 0
      }
      lastPageNum = pageNum

      const item = {
        id: `${collection.uuid}.${line.id}`,
        collection_id: collection.uuid,
        organization_id: ORGANIZATION_ID,
        data:{
          page_num: pageNum,
          bbox: line.bbox,
          text: line.text
        }
      }

      i += 1
      return item
    })
    .filter((line) => i <= 10)
}

H(COLLECTIONS)
  .map((collection) => Object.assign(collection, {
    lines: path.join(cityDirectoriesDir, collection.dir, 'lines.ndjson')
  }))
  .filter((collection) => fs.existsSync(collection.lines))
  .map((collection) => createLinesStream(collection))
  .flatten()
  .toArray((items) => {
    brickDb.addAll(tasks, collections, items, true)
  })
