var thunky = require('thunky')
var lexint = require('lexicographic-integer')
var from = require('from2')
var bulk = require('bulk-write-stream')

var noop = function () {}

var deleted = function (value) {
  return value && value.length === 1 && value[0] === 0x20
}

var checkSeek = function (ite) {
  if (ite.seek) return
  throw new Error('Sorry! rollbackdb currently only works with the latest version of leveldown. This might change in the future')
}

var rollbackdb = function (db, opts) {
  if (!opts) opts = {}

  var that = {}
  var freeIterators = []
  var pf = opts.prefix ? '!' + opts.prefix + '!' : ''

  var open = thunky(function (cb) {
    var onopen = function () {
      var ite = db.db.iterator({
        gt: pf + 'changes!',
        lt: pf + 'changes!\xff',
        reverse: true,
        limit: 1
      })
      ite.next(function (err, key) {
        ite.end(function () {
          if (err) return cb(err)
          that.changes = key ? lexint.unpack(key.toString().slice(pf.length + 'changes!'.length).split('!')[0], 'hex') : 0
          cb(null, db.db, db)
        })
      })
    }

    if (db.isOpen()) onopen()
    else db.open(onopen)
  })

  that.version = opts.checkout || opts.version || 0
  that.changes = 0

  that.checkout = function (change) {
    return rollbackdb(db, {checkout: change})
  }

  that.del = function (key, cb) {
    that.put(key, ' ', cb) // use ' ' as whiteout for now ...
  }

  that.put = function (key, value, cb) {
    if (!cb) cb = noop
    open(function (err, down) {
      if (err) return cb(err)

      var change = ++that.changes
      var dbKey = pf + 'data!' + key + '!' + lexint.pack(change, 'hex') + '!'

      down.batch([{
        type: 'put',
        key: pf + 'changes!' + lexint.pack(change, 'hex') + '!' + lexint.pack(0, 'hex'),
        value: dbKey
      }, {
        type: 'put',
        key: dbKey,
        value: value
      }], cb)
    })
  }

  that.status = function (cb) {
    open(function (err) {
      if (err) return cb(err)
      cb(null, {changes: that.changes})
    })
  }

  that.batch = function (batch, opts, cb) {
    if (typeof opts === 'function') return that.batch(batch, null, opts)
    if (!opts) opts = {}
    if (!cb) cb = noop

    open(function (err, down) {
      if (err) return cb(err)

      var change = opts.change || (that.changes + 1)
      if (!opts.tick) opts.tick = 0
      if (change > that.changes) that.changes = change

      var wrap = new Array(2 * batch.length)
      var suffix = '!' + lexint.pack(change, 'hex') + '!'
      var changePrefix = pf + 'changes!' + lexint.pack(change, 'hex') + '!'
      var dataPrefix = pf + 'data!'

      for (var i = 0; i < batch.length; i++) {
        var key = batch[i].key
        var tick = lexint.pack(opts.tick++, 'hex')
        var dbKey = dataPrefix + key + suffix + tick

        wrap[2 * i] = {
          type: 'put',
          key: dbKey,
          value: batch[i].value || ' '
        }
        wrap[2 * i + 1] = {
          type: 'put',
          key: changePrefix + tick,
          value: dbKey
        }
      }

      down.batch(wrap, cb)
    })
  }

  var free = function (ite) {
    if (freeIterators.length < 10) freeIterators.push(ite)
    else ite.end(noop)
  }

  that.get = function (key, opts, cb) {
    if (typeof opts === 'function') return that.get(key, null, opts)
    if (!opts) opts = {}

    open(function (err, down) {
      if (err) return cb(err)

      var ite = freeIterators.shift() || down.iterator({reverse: true})
      var prefix = pf + 'data!' + key + '!'

      checkSeek(ite)
      ite.seek(prefix + lexint.pack(opts.version || that.version || that.changes, 'hex') + '!~')
      ite.next(function (err, key, value) {
        if (err) return cb(err)
        free(ite)
        if (key && key.toString('utf-8', 0, prefix.length) !== prefix) return cb(null, null)
        if (deleted(value)) return cb(null, null)
        cb(null, value || null)
      })
    })
  }

  that.createChangesStream = function (opts) {
    if (!opts) opts = {}

    var openIterator = thunky(function (cb) {
      open(function (err, down) {
        if (err) return cb(err)

        var prefix = pf + 'changes!'
        if (opts.change) prefix += lexint.pack(opts.change, 'hex') + '!'

        var ite = down.iterator({
          gte: opts.gt === undefined ? (prefix + (opts.gte !== undefined ? lexint.pack(opts.gte, 'hex') : '')) : undefined,
          lte: opts.lt === undefined ? (prefix + (opts.lte !== undefined ? lexint.pack(opts.lte, 'hex') + '!~' : '~')) : undefined,
          gt: opts.gte === undefined ? (prefix + (opts.gt !== undefined ? lexint.pack(opts.gt, 'hex') + '!~' : '')) : undefined,
          lt: opts.lte === undefined ? (prefix + (opts.lt !== undefined ? lexint.pack(opts.lt, 'hex') : '~')) : undefined
        })

        cb(null, ite, down)
      })
    })

    var end = function (ite, cb) {
      ite.end(function () {
        cb(null, null)
      })
    }

    return from.obj(function (size, cb) {
      openIterator(function (err, ite, down) {
        if (err) return cb(err)

        ite.next(function (err, key, value) {
          if (err) return cb(err)
          if (!key) return end(ite, cb)

          key = key.toString()

          var i = key.lastIndexOf('!')
          var index = lexint.unpack(key.slice(i + 1), 'hex')
          var j = key.lastIndexOf('!', i - 1)
          var change = lexint.unpack(key.slice(j + 1, i), 'hex')

          var dbKey = value.toString()
          i = dbKey.lastIndexOf('!', dbKey.lastIndexOf('!') - 1)
          j = dbKey.lastIndexOf('!', i - 1)

          down.get(dbKey, function (err, val) {
            if (err) return cb(err)

            cb(null, {
              change: change,
              index: index,
              key: dbKey.slice(j + 1, i),
              value: deleted(val) ? null : val
            })
          })
        })
      })
    })
  }

  that.createWriteStream = function () {
    var opts = {change: 0, tick: 0}
    return bulk.obj(function (batch, cb) {
      open(function () {
        if (!opts.change) opts.change = that.changes + 1
        that.batch(batch, opts, cb)
      })
    })
  }

  that.createReadStream = function (opts) {
    if (!opts) opts = {}

    var openIterator = thunky(function (cb) {
      open(function (err, down) {
        if (err) return cb(err)
        var forward = down.iterator({highWaterMark: 1})
        var backward = down.iterator({reverse: true, highWaterMark: 1})
        checkSeek(forward)
        cb(null, forward, backward)
      })
    })

    var next = pf + 'data!'
    if (opts.gt) next = pf + 'data!' + opts.gt + '!~'
    if (opts.gte) next = pf + 'data!' + opts.gte + '!'

    var lt = opts.lt && pf + 'data!' + opts.lt + '!'
    var lte = opts.lte && pf + 'data!' + opts.lte + '!'

    var end = function (cb) {
      openIterator(function (err, backward, forward) {
        if (err) return cb(err)
        backward.end(function () {
          forward.end(function () {
            cb(null, null)
          })
        })
      })
    }

    return from.obj(function read (size, cb) {
      openIterator(function (err, forward, backward) {
        if (err) return cb(err)

        var checkout = opts.version || that.version || that.changes

        forward.seek(next)
        forward.next(function (err, key, val) {
          if (err) return cb(err)
          if (!key) return end(cb)

          key = key.toString()
          var i = key.indexOf('!', 5 + pf.length) // data!
          var prefix = key.slice(0, i + 1)

          if (lt && prefix >= lt) return end(cb)
          if (lte && prefix > lte) return end(cb)

          next = prefix + '~'
          backward.seek(prefix + lexint.pack(checkout, 'hex') + '!~')
          backward.next(function (err, key, value) {
            if (key.toString('utf-8', 0, prefix.length) !== prefix) return read(size, cb)
            if (deleted(value)) return read(size, cb)
            if (lte && prefix === lte) lt = lte

            cb(null, {
              key: key.toString('utf-8', 5 + pf.length, i),
              value: value
            })
          })
        })
      })
    })
  }

  return that
}

module.exports = rollbackdb
