#!/usr/bin/env node

var fs       = require('fs')
var os       = require('os')
var path     = require('path')
var chpro    = require('child_process')

var through  = require('through')
var csv      = require('csv-stream')
var osenv    = require('osenv')
var duplexer = require('duplexer')
var concat   = require('concat-stream')

var Transform = require('stream').Transform;

var spawn = chpro.spawn
if (os.type() === 'Windows_NT') spawn = require('win-spawn')

var SkipLines = function(lines) {
  this.toskip = lines;
  Transform.call(this,{});
};

require('util').inherits(SkipLines,Transform);

SkipLines.prototype._transform = function(chunk,enc,cb) {
  if (this.toskip < 0) {
    this.push(chunk);
    cb();
    return;
  }
  var data = chunk.toString();
  while (this.toskip > 0 && data.indexOf('\n') >= 0) {
    this.toskip--;
    data = data.substring(data.indexOf('\n')+1);
  };
  if (this.toskip === 0) {
    this.push(data);
  }
  cb();
};

module.exports = function (options) {

  var read = through()
  var duplex

  var filename = path.join(osenv.tmpdir(), '_'+require('node-uuid').v4())

  var spawnArgs = []

  if (options) {
    options.sheet && spawnArgs.push('--sheet') && spawnArgs.push(options.sheet) && delete options.sheet
    options.sheetIndex && spawnArgs.push('--sheet-index') && spawnArgs.push(options.sheetIndex) && delete options.sheetIndex
  }

  spawnArgs.push(filename)

  var write = fs.createWriteStream(filename)
    .on('close', function () {
      var child = spawn(require.resolve('j/bin/j.njs'), spawnArgs)
      child.stdout.pipe(new SkipLines((options.startRow || 1) - 1)).pipe(csv.createStream(options))
        .pipe(through(function (data) {
          var _data = {}
          for(var k in data) {
            var value = data[k].trim()
            _data[k.trim()] = isNaN(value) ? value : +value
          }
          this.queue(_data)
        }))
        .pipe(read)
      child.on('exit', function(code, sig) {
        if(code === null || code !== 0) {
          child.stderr.pipe(concat(function(errstr) {
            duplex.emit('error', new Error(errstr))
          }))
        }
      })
    })

  return (duplex = duplexer(write, read))

}


if(!module.parent) {
  var JSONStream = require('JSONStream')
  var args = require('minimist')(process.argv.slice(2))
  process.stdin
    .pipe(module.exports())
    .pipe(args.lines || args.newlines
      ? JSONStream.stringify('', '\n', '\n', 0)
      : JSONStream.stringify()
    )
    .pipe(process.stdout)
}
