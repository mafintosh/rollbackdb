# rollbackdb

A simple key/value database with fast rollback support.

```
npm install rollbackdb
```

![dat](http://img.shields.io/badge/Development%20sponsored%20by-dat-green.svg?style=flat)

## Usage

``` js
var rollbackdb = require('rollbackdb')
// OBS: currently you have use the latest leveldown (>=1.2.0)
var db = require('levelup')('db', {db: require('leveldown')})

var rdb = rollbackdb(db)

rdb.put('hello', 'world', function () {
  var oldChange = rdb.changes
  console.log('added hello world at change', rdb.changes)
  rdb.put('hello', 'not world', function () {
    console.log('added hello not world at change', rdb.changes)
    rdb.get('hello', {version: oldChange}, console.log) // prints 'world'
  })
})
```

## API

#### `rdb = rollbackdb(levelup, [options])`

Create a new rollbackdb instance. You can set `version` in options
if you want to rollback the database to a previous version

#### `checkoutRdb = rdb.checkout(version)`

Shorthand for setting the `version` option in the constructor.
Returns a new instance.

#### `rdb.get(key, [options], cb)`

Get a key. Set `version` in the options map to get the key that
was added in a previous version of this database.

#### `rdb.put(key, value, [cb])`

Insert a key.

#### `rdb.del(key, [cb])`

Delete a key.

#### `rdb.batch(batch, [cb])`

Insert and/or deletes a batch of key/values

``` js
rdb.batch([{
  type: 'put',
  key: 'a',
  value: 'a'
}, {
  type: 'del',
  key: 'b',
  value: 'b'
}], function (err) {
  console.log('batch finished')
})
```

#### `rdb.changes`

A property containing the total number of changes/versions added to this database

#### `rs = rdb.createReadStream([options])`

Create a readable stream of all keys and values in the database.
Options include

``` js
{
  gt: 'keys-must-be-greater-than-me',
  gte: 'keys-must-be-greater-or-equal-to-me',
  lt: 'keys-must-be-less-than-me',
  lte: 'keys-must-be-less-or-equal-to-me',
  version: aVersionNumber // read all values in the database at this version
}
```

#### `ws = rdb.createWriteStream()`

Returns a writable stream that you can write `{type: 'put'|'del', key: key, value: value}` pairs to.
All values written will be added to the same version.

## License

MIT
