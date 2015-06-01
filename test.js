var tape = require('tape')
var rollbackdb = require('./')
var leveldown = require('leveldown')
var levelup = require('levelup')
var rimraf = require('rimraf')
var os = require('os')
var path = require('path')

var tick = 0
var prev

var create = function (opts) {
  var tmp = path.join(os.tmpdir(), 'rollbackdb-' + process.pid + '-' + ++tick)
  rimraf.sync(tmp)
  if (prev) rimraf.sync(prev)
  prev = tmp
  return rollbackdb(levelup(tmp, {db: leveldown}), opts)
}

tape('put + get', function (t) {
  var db = create()
  db.put('hello', 'world', function (err) {
    t.error(err, 'no error')
    t.same(db.changes, 1, 'one change')
    db.get('hello', function (err, val) {
      t.same(val.toString(), 'world')
      t.end()
    })
  })
})

tape('put + put + get', function (t) {
  var db = create()
  db.put('hello', 'world', function (err) {
    t.error(err, 'no error')
    db.put('hello', 'not world', function (err) {
      db.get('hello', function (err, val) {
        t.same(val.toString(), 'not world')
        t.end()
      })
    })
  })
})

tape('put + put + checkout get', function (t) {
  var db = create()
  db.put('hello', 'world', function (err) {
    t.error(err, 'no error')
    db.put('hello', 'not world', function (err) {
      db.get('hello', {version: 1}, function (err, val) {
        t.same(val.toString(), 'world')
        t.end()
      })
    })
  })
})

tape('readstream', function (t) {
  var db = create()
  db.batch([{
    type: 'put',
    key: 'a',
    value: 'a'
  }, {
    type: 'put',
    key: 'b',
    value: 'b'
  }, {
    type: 'put',
    key: 'c',
    value: 'c'
  }], function (err) {
    t.error(err, 'no error')

    var rs = db.createReadStream()
    var expected = ['a', 'b', 'c']

    rs.on('data', function (data) {
      t.same(expected.shift(), data.value.toString())
    })

    rs.on('end', function () {
      t.same(expected.length, 0, 'no more data')
      t.end()
    })
  })
})

tape('readstream override', function (t) {
  var db = create()
  db.batch([{
    type: 'put',
    key: 'a',
    value: 'a'
  }, {
    type: 'put',
    key: 'b',
    value: 'b'
  }, {
    type: 'put',
    key: 'c',
    value: 'c'
  }], function (err) {
    t.error(err, 'no error')

    db.put('b', 'B', function () {
      var rs = db.createReadStream()
      var expected = ['a', 'B', 'c']

      rs.on('data', function (data) {
        t.same(expected.shift(), data.value.toString())
      })

      rs.on('end', function () {
        t.same(expected.length, 0, 'no more data')
        t.end()
      })
    })
  })
})

tape('readstream override prefix', function (t) {
  var db = create({prefix: 'test'})
  db.batch([{
    type: 'put',
    key: 'a',
    value: 'a'
  }, {
    type: 'put',
    key: 'b',
    value: 'b'
  }, {
    type: 'put',
    key: 'c',
    value: 'c'
  }], function (err) {
    t.error(err, 'no error')

    db.put('b', 'B', function () {
      var rs = db.createReadStream({gt: 'a'})
      var expected = ['B', 'c']

      rs.on('data', function (data) {
        t.same(expected.shift(), data.value.toString())
      })

      rs.on('end', function () {
        t.same(expected.length, 0, 'no more data')
        t.end()
      })
    })
  })
})

tape('readstream checkout', function (t) {
  var db = create()
  db.batch([{
    type: 'put',
    key: 'a',
    value: 'a'
  }, {
    type: 'put',
    key: 'b',
    value: 'b'
  }, {
    type: 'put',
    key: 'c',
    value: 'c'
  }], function (err) {
    t.error(err, 'no error')

    db.put('b', 'B', function () {
      var rs = db.createReadStream({version: 1})
      var expected = ['a', 'b', 'c']

      rs.on('data', function (data) {
        t.same(expected.shift(), data.value.toString())
      })

      rs.on('end', function () {
        t.same(expected.length, 0, 'no more data')
        t.end()
      })
    })
  })
})

tape('cleanup', function (t) {
  if (prev) rimraf.sync(prev)
  t.ok(true, 'all cleaned up')
  t.end()
})