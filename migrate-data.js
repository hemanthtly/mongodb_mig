const mongodb = require('mongodb')
const async = require('async')
const path = require('path')
const customers = require(path.join(__dirname, 'm3-customer-data.json'))
const address = require(path.join(__dirname, 'm3-customer-address-data.json'))

const url = 'mongodb://localhost:27017'

var argv = 100
var recordcount = customers.length
var task = []

if (process.argv.length == 3){
    argv = parseInt(process.argv[2])
}
const limit = recordcount / argv
console.log(`Parallel count ${limit}`)

for (var i = 0; i < recordcount; i++){
    customers[i] = Object.assign(customers[i], address[i])
}

mongodb.MongoClient.connect(url, (error, client) => {
    if (error) return process.exit(1)
    const db = client.db('edx-course-db')

    console.log('Database connection OK!')

    const collection = db.collection('customers')

    for (var i = 0; i < limit; i++){
        let start = i * argv
        let end = start + argv

        task.push((callback) => {
            collection.insert(customers.slice(start, end), (error, data) => {
                if (error) throw error
            callback(error, data)
            })
        })
    }

    async.parallel(task, (error, data) => {
        if (error) throw error

    console.log('All records inserted!')
    client.close()
    console.log('Database connection closed!')
    })
})