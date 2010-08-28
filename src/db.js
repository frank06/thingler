var db = require('riak-js').getClient({clientId: 'thingler'}),
  bucket = 'thingler';
  
// riak adapter -- do our best not to alter the rest of the code

module.exports = {
  database: {
    get: function(id, callback) {
      db.get(bucket, id)(function(result, meta) {
        result = adapt(id, result, meta);
        callback(meta.statusCode >= 400 ? result : null, result)
      });
    },
    put: function(id, doc, callback) {
      db.save(bucket, id, doc, {returnbody: true})(function(result, meta) {
        result = adapt(id, result, meta);
        callback(meta.statusCode >= 400 ? result : null, result)
      });
    }
  },
  parseRev: function (rev) {
      // FIXME dummy function until we figure out a proper riak version with vclocks
      return rev;
  },
  connection: {
      // Riak doesn't have a generate uuids endpoint, so doing that locally -- could definitely be better
      uuids: function(total, callback) {
        var uuids = [];
        for (var i=0; i < total; i++) {
          uuids.push(uuid())
        }
        callback(null, uuids);
      }
    }
};

function adapt(id, result, meta) {
  result = meta.statusCode === 404 ? {error: "not_found", headers: { status: meta.statusCode }} : result;
  result._id = id;
  result._rev = meta.headers['X-Riak-Vclock'];
  result.headers = meta.headers;
  result.headers.status = meta.statusCode;
  return result;
}

function uuid() {
  function S4(){
    return (((1+Math.random())*0x10000)|0).toString(16).substring(1);
  }
  return (S4()+S4()+S4()+S4()+S4()+S4()+S4()+S4());
}