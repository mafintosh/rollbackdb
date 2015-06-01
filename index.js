var thunky = require('thunky')
var lexint = require('lexicographic-integer')
var from2 = require('from2')
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
      db.get('changes', {valueEncoding: 'utf-8'}, function (_, changes) {
        that.changes = parseInt(changes || '0', 10)
        cb(null, db.db)
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

      down.batch([{
        type: 'put',
        key: pf + 'changes',
        value: change.toString()
      }, {
        type: 'put',
        key: pf + 'data!' + key + '!' + lexint.pack(change, 'hex'),
        value: value
      }], cb)
    })
  }

  that.batch = function (batch, opts, cb) {
    if (typeof opts === 'function') return that.batch(batch, null, opts)
    if (!opts) opts = {}
    if (!cb) cb = noop

    open(function (err, down) {
      if (err) return cb(err)

      var change = opts.change || (that.changes + 1)
      var wrap = new Array((change > that.changes) ? batch.length + 1 : batch.length)
      var suffix = '!' + lexint.pack(change, 'hex')

      for (var i = 0; i < batch.length; i++) {
        wrap[i] = {
          type: 'put',
          key: pf + 'data!' + batch[i].key + suffix,
          value: batch[i].value || ' '
        }
      }

      if (change > that.changes) {
        that.changes = change
        wrap[wrap.length - 1] = {
          type: 'put',
          key: pf + 'changes',
          value: change.toString()
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
      ite.seek(prefix + lexint.pack(opts.version || that.version || that.changes, 'hex'))
      ite.next(function (err, key, value) {
        if (err) return cb(err)
        free(ite)
        if (key && key.toString('utf-8', 0, prefix.length) !== prefix) return cb(null, null)
        if (deleted(value)) return cb(null, null)
        cb(null, value || null)
      })
    })
  }

  that.createWriteStream = function () {
    var change = 0
    return bulk.obj(function (batch, cb) {
      open(function () {
        if (!change) change = that.changes + 1
        that.batch(batch, {change: change}, cb)
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

    return from2.obj(function read (size, cb) {
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
          backward.seek(prefix + lexint.pack(checkout, 'hex'))
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
