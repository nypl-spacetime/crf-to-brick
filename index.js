const fs = require('fs')
const path = require('path')
const H = require('highland')

const surveyorDb = require('to-surveyor')

const PROVIDER = 'nypl'
const DATA_DIR = path.join(__dirname, 'data')

const collections = fs.readdirSync(DATA_DIR)
    .filter((file) => fs.statSync(path.join(DATA_DIR, file)).isDirectory())

collections.forEach((collectionId) => {
  const collection = {
    provider: PROVIDER,
    id: collectionId,
    tasks: [
      'label-fields'
    ],
    submissions_needed: 1, // submissionsNeeded: -1 means endless!!!!
    data: require(path.join(DATA_DIR, collectionId, 'collection.json'))
  }

  surveyorDb.addCollection(collection, (err) => {
    if (err) {
      console.error(`Error adding collection ${collectionId}: ${err.message}`)
    } else {
      console.log(`Done adding collection ${collectionId}`)
      H(fs.createReadStream(path.join(DATA_DIR, collectionId, 'lines.ndjson')))
        .split()
        .compact()
        .map(JSON.parse)
        .map((line) => ({
          provider: PROVIDER,
          id: `${collectionId}:${line.id}`,
          collection_id: collectionId,
          data: {
            text: line.text,
            bbox: line.bbox,
            pageNum: line.page_num
          }
        }))
        .toArray((items) => {
          surveyorDb.addItems(items, (err) => {
            if (err) {
              console.error(`Error adding items for collection ${collectionId}: ${err.message}`)
            } else {
              console.log(`Done adding ${items.length} items for collection ${collectionId}`)
            }
          })
        })
    }
  })
})
