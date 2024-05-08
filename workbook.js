// ASP Workbook
/////////////
//https://github.com/josephxsxn/atlas_stream_processors/blob/master/example_processors

// compass query
// which category_id has the most reviews, joining the category_id field with the id in the categories collection



////////////////////////
//CHANGE STREAMS
////////////////////////

//bring up two update ruby scripts
ruby wolt_update_products.rb -h "mongodb+srv://<user>:<pass>@wolt-devloper-day.9mxnc.mongodb.net/" -c "products"

// run the ruby watch script:

// 1. watch the whole collection for changes
// 2. watch for start ratings above a set amount
// 3. watch for a specific reviewer



//enable pre and post images on change streams (in compass)
use sales

db.runCommand( {
  collMod: "products",
  changeStreamPreAndPostImages: { enabled: true }
} )



// create something in the connection registry


// connect to the stream processor using a new command shell
mongosh "mongodb://atlas-stream-6632070cbe12d51793836407-9mxnc.virginia-usa.a.query.mongodb.net/" --tls --authenticationDatabase admin --username jim

//list connections 
sp.listConnections()




//Test to see if the data source emits messages

s1={
  "$source": {
    "connectionName": "Wolt-products",
    "db" : "sales",
    "coll" : "products",
  }
}

sp.process([s1])


// unwind array
s1 = { "$source": { "connectionName": "Wolt-products", "db": "sales", "coll": "products" } }
u = {$unwind: {path: "$updateDescription.updatedFields.reviews"}}
v = {$validate: {validator: {$expr: {$gte:["$updateDescription.updatedFields.reviews.stars",2]}}}}
sp.process([s1, u, v])

//v = {$validate: {validator: {$expr: {"$fullDocument.category_id":{$in:[110,114]}}}}}
//v = {$validate: {validator: {$expr: {$in: ["category_id",110]}}}}




// return full document
s2 = {
  '$source': {
    connectionName: 'Wolt-products',
    db: 'sales',
    coll: 'products',
    config: { fullDocument: 'required' }
  }
}

u = {$unwind: {path: "$fullDocument.reviews"}}


// basic tumbling window - count number of reviews, and average the star rating,  over a 30 second tumbling window

t = {
  $tumblingWindow: {
      interval: {size: NumberInt(30), unit: "second"},
      pipeline: [
        {
          $group : {
            _id : {"title" : "$fullDocument.title","asin" : "$fullDocument.asin","category_id" : "$fullDocument.category_id"},
            averageStars: { $avg: "$fullDocument.reviews.stars" },
            numberOfReviews: { $sum: 1 },
         }
        },
          { $project: { 
              _id : 1,
              averageStars: 1,
              numberOfReviews: 1
              }
          }
      ]
  }
}

// group by category - top ten most busy categories - using a lookup to the category collection

t = {
  $tumblingWindow: {
      interval: {size: NumberInt(10), unit: "second"},
      pipeline: [
        {
          $group : {
            _id : {"category_id" : "$fullDocument.category_id"},
            averageStars: { $avg: "$fullDocument.reviews.stars" },
            numberOfReviews: { $sum: 1 },
         }
        },
        {
          $sort: {numberOfReviews:1}
        },
        {
          $limit: 10
        },
        { $project: { 
            "_id.category_id": 1,
            averageStars: 1,
            numberOfReviews: 1
            }
        }
      ]
  }
}

l = {
  $lookup: {
    from: {
        connectionName: 'Wolt-products',
        db: 'sales',
        coll: 'categories'
    },
      localField: "_id.category_id",
      foreignField: "id",
      as: "category"
    }
}

sp.process([s2,u,t,l])



