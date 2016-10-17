const http = require('http');
const https = require('https');
const url = require('url');
let h;

export function createClient(uri, databaseOptions) {
  return new CouchClient(uri, databaseOptions);
}

export function normalizePerms(object) {
  if (!object) return object;

  if (object._rperm) {
    if (object._rperm instanceof Array) {
      object.rperm = object._rperm.slice(0);
    } else {
      object.rperm = Object.assign({}, object._rperm);
    }
    delete object._rperm;
  }
  if (object._wperm) {
    if (object._wperm instanceof Array) {
      object.wperm = object._wperm.slice(0);
    } else {
      object.wperm = Object.assign({}, object._wperm);
    }
    delete object._wperm;
  }
  if (object._hashed_password) {
    if (object._hashed_password instanceof Array) {
      object.hashed_password = object._hashed_password.slice(0);
    } else if (typeof object._hashed_password === 'string') {
      object.hashed_password = object._hashed_password;
    } else {
      object.hashed_password = Object.assign({}, object._hashed_password);
    }
    delete object._hashed_password;
  }

  return object;
}

// Key:tlynimsemserearnothatful
// Password:56c64239779b16c3fd1db5afc309139883faa192

// Url {
//   protocol: 'http:',
//   slashes: true,
//   auth: null,
//   host: 'localhost:5984',
//   port: '5984',
//   hostname: 'localhost',
//   hash: null,
//   search: '?Sdf',
//   query: 'Sdf',
//   pathname: '/parse-test',
//   path: '/parse-test?Sdf',
//   href: 'http://localhost:5984/parse-test?Sdf' }

class CouchClient {
  constructor(uri, databaseOptions) {
    this.parsedURL = url.parse(uri);
    this.requestOptions = {
      protocol: this.parsedURL.protocol,
      auth: this.parsedURL.auth,
      hostname: this.parsedURL.hostname,
      port: parseInt(this.parsedURL.port) || 5984,
      path: this.parsedURL.pathname,
      headers: {
        'Content-Type': 'application/json'
      }
    }
    this.databaseOptions = databaseOptions;
    h = this.requestOptions.protocol == 'https:' ? https : http;
  }

  all(type, limit) {
    return this._makeHTTPRequest('/_find', 'POST', {
      selector: {
        $t: {
          '$eq': type
        }
      },
      limit: limit ? limit : 50
      // }).then(res => res.length >= 0 ? res : [res]);
    }).then(res => res.docs);
  }

  get(id) {
    return this._makeHTTPRequest(`/${id}`, 'GET');
  }

  update(doc) {
    return this._makeHTTPRequest('/${doc._id}', 'PUT', doc);
  }

  bulkUpdate(docs) {
    return this._makeHTTPRequest('/_bulk_docs', 'POST', docs);
  }

  updateById(id, obj) {
    return this.get(id)
      .then(doc => {
        doc = Object.assign(doc, obj);
        return this.update(doc);
      })
  }

  query(selector) {
    return this._makeHTTPRequest('/_find', 'POST', selector)
      .then(res => {
        if (res.warning) {
          console.log(res.warning);
          console.log('Selector used: ', selector);
        }
        return res;
      })
      .then(res => {
        if (selector.selector && selector.selector.$t === '_User') {
          if (res.hashed_password) {
            res._hashed_password = res.hashed_password;
            delete res.hashed_password;
          } else if (res.docs) {
            res.docs.forEach(d => {
              d._hashed_password = d.hashed_password;
              delete d.hashed_password;
            });
          }
        }
        return res;
      })
      .then(res => res.length >= 0 || res.error || res.docs ? res : [res]);
  }

  create(doc) {
    doc = normalizePerms(doc);
    return this._makeHTTPRequest('', 'POST', doc);
  }

  createClass(doc) {
    const id = `class:${doc.className}`;
    return this._makeHTTPRequest(`/${id}`, 'PUT', doc)
      .then(res => {
        if (res.error === 'conflict') {
          return doc;
        }
      })
  }

  delete(id) {
    return this.get(id)
      .then(doc => {
        doc._deleted = true;
        this._makeHTTPRequest('', 'POST', doc)
      });
  }

  deleteClass(className) {
    const p1 = this.query({
      selector: {
        $t: className
      },
      fields: ['_id', '_rev']
    }).then(res => {
      res.docs.forEach(d => {
        d._deleted = true;
      });
      return this._makeHTTPRequest('/_bulk_docs', 'POST', res);
    });

    const p2 = this.get(`class:${className}`)
      .then(doc => {
        doc._deleted = true;
        return this._makeHTTPRequest('/class:${className}',
          'PUT',
          doc)
      });

    return Promise.all([p1, p2]);
  }

  deleteAll() {
    return this._makeHTTPRequest('/_all_docs', 'GET')
      .then(res => {
        if (res.rows.length > 0) {
          res.rows.forEach(r => {
            r._deleted = true;
          });
          return this.bulkUpdate({ docs: res.rows });
        }
        return;
      });
  }

  _makeHTTPRequest(path, method, payload) {
    return new Promise((resolve, reject) => {
      const options = Object.assign({}, this.requestOptions, {
        path: `${this.requestOptions.path}${path}`,
        method
      });

      const req = h.request(options, (res) => {
        let body = '';
        res.setEncoding('utf8');
        res.on('data', (chunk) => {
          body += chunk;
        });
        res.on('end', () => {
          resolve(JSON.parse(body));
        });
      });

      req.on('error', reject);

      if (payload) {
        req.write(JSON.stringify(payload));
      }

      req.end();
    });
  }
}

