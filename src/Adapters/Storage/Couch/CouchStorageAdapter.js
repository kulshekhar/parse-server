import { createClient, normalizePerms } from './CouchClient';

const logger = require('../../../logger');

export class CouchStorageAdapter {
  _client;

  constructor({
    uri,
    databaseOptions
  }) {
    this._client = createClient(uri, databaseOptions);
  }

  _ensureSchemaCollectionExists() {
    return success();
  };

  classExists(name) {
    return this._client.get(`class:${name}`);
  }

  setClassLevelPermissions(className, CLPs) {
    return this._client.get(`class:${className}`)
      .then(doc => {
        doc.classLevelPermissions = CLPs;
        return this._client.update(doc);
      });
  }

  createClass(className, schema) {
    schema.fields._id = { type: 'String' };
    schema.fields._rev = { type: 'String' };

    return this._client.createClass(Object.assign({}, schema, {
      $t: '_SCHEMA',
      isParseClass: true
    }))
      .then(() => toParseSchema(schema));
  }

  // Just create a table, do not insert in schema
  createTable(className, schema) {
    return success();
  }

  addFieldIfNotExists(className, fieldName, type) {
    // // TODO: Must be revised for invalid logic...
    debug('addFieldIfNotExists', { className, fieldName, type });

    return this._client.get(`class:${className}`)
      .then(doc => {
        if (doc.fields[fieldName]) {
          throw "Attempted to add a field that already exists";
        }
        doc.fields[fieldName] = type;
        return this._client.update(doc);
      });
  }

  // Drops a collection. Resolves with true if it was a Parse Schema (eg. _User, Custom, etc.)
  // and resolves with false if it wasn't (eg. a join table). Rejects if deletion was impossible.
  deleteClass(className) {
    return this._client.deleteClass(className);
  }

  // Delete all data known to this adapter. Used for testing.
  deleteAllClasses() {
    debug('deleteAllClasses');
    this._client.deleteAll();
  }

  // Remove the column and all the data. For Relations, the _Join collection is handled
  // specially, this function does not delete _Join columns. It should, however, indicate
  // that the relation fields does not exist anymore. In mongo, this means removing it from
  // the _SCHEMA collection.  There should be no actual data in the collection under the same name
  // as the relation column, so it's fine to attempt to delete it. If the fields listed to be
  // deleted do not exist, this function should return successfully anyways. Checking for
  // attempts to delete non-existent fields is the responsibility of Parse Server.

  // This function is not obligated to delete fields atomically. It is given the field
  // names in a list so that databases that are capable of deleting fields atomically
  // may do so.

  // Returns a Promise.
  deleteFields(className, schema, fieldNames) {
    debug('deleteFields', className, fieldNames);

    fieldNames = fieldNames.filter(f => f !== '_id' && f !== '_rev');

    if (fieldNames.length === 0) return Promise.resolve();

    const p1 = this._client.get(`class:${className}`)
      .then(doc => {
        fieldNames.forEach(f => {
          delete doc.fields[f];
        });
        return this._client.update(doc);
      });

    const selector = {
      $t: className,
      '$or': fieldNames ? fieldNames.map(f => {
        const obj = {};
        obj[f] = { '$gt': null }
        return obj;
      }) : []
    };

    const p2 = this._client.query({ selector })
      .then(res => {
        if (res.docs && res.docs.length > 0) {
          res.docs.forEach(d => {
            fieldNames.forEach(f => {
              delete d[f];
            });
          });
          return this._client.bulkUpdate(res);
        }
        return;
      })

    return Promise.all([p1, p2]);
  }

  // Return a promise for all schemas known to this adapter, in Parse format. In case the
  // schemas cannot be retrieved, returns a promise that rejects. Requirements for the
  // rejection reason are TBD.
  getAllClasses() {
    debug('getAllClasses');
    return this._ensureSchemaCollectionExists()
      .then(() => this._client.all('_SCHEMA'))
      .then(schemas => schemas ? schemas.map(toParseSchema) : []);
  }

  // Return a promise for the schema with the given name, in Parse format. If
  // this adapter doesn't know about the schema, return a promise that rejects with
  // undefined as the reason.
  getClass(className) {
    debug('getClass', className);
    return this._client.get(`class:${className}`);
  }

  // TODO: remove the mongo format dependency in the return value
  createObject(className, schema, object) {
    debug('createObject', className, object);

    schema.fields = normalizePerms(schema.fields);
    object = normalizePerms(object);

    Object.keys(object).forEach(fieldName => {
      var authDataMatch = fieldName.match(/^_auth_data_([a-zA-Z0-9_]+)$/);
      if (authDataMatch) {
        var provider = authDataMatch[1];
        object['authData'] = object['authData'] || {};
        object['authData'][provider] = object[fieldName];
        delete object[fieldName];
        fieldName = 'authData';
      }

      switch (schema.fields[fieldName].type) {
        case 'Date':
          object[fieldName] = object[fieldName].iso
          break;
        case 'Pointer':
          object[fieldName] = object[fieldName].objectId;
          break;
        case 'Array':
          // if (['_rperm', '_wperm'].indexOf(fieldName) >= 0) {
          //   // valuesArray.push(object[fieldName]);
          // } else {
          //   // valuesArray.push(JSON.stringify(object[fieldName]));
          // }
          break;
        case 'Object':
        case 'String':
        case 'Number':
        case 'Boolean':
          break;
        case 'File':
          object[fieldName] = object[fieldName].name;
          break;
        case 'GeoPoint':
          // pop the point and process later
          geoPoints[fieldName] = object[fieldName];
          break;
        default:
          throw `Type ${schema.fields[fieldName].type} not supported yet`;
      }
    });


    // let geoPointsInjects = Object.keys(geoPoints).map((key, idx) => {
    //   let value = geoPoints[key];
    //   valuesArray.push(value.longitude, value.latitude);
    //   let l = valuesArray.length + columnsArray.length;
    //   return `POINT($${l}, $${l+1})`;
    // });

    object._id = object.objectId;
    object.$t = className;
    return this._client.create(object)
      .then(() => ({ ops: [object] }));
  }

  // Remove all objects that match the given Parse Query.
  // If no objects match, reject with OBJECT_NOT_FOUND. If objects are found and deleted, resolve with undefined.
  // If there is some other error, reject with INTERNAL_SERVER_ERROR.
  deleteObjectsByQuery(className, schema, query) {
    // debug('deleteObjectsByQuery', className, query);
    // let values = [className];
    // let index = 2;
    // let where = buildWhereClause({ schema, index, query })
    // values.push(...where.values);
    // if (Object.keys(query).length === 0) {
    //   where.pattern = 'TRUE';
    // }
    // let qs = `WITH deleted AS (DELETE FROM $1:name WHERE ${where.pattern} RETURNING *) SELECT count(*) FROM deleted`;
    // debug(qs, values);
    // return this._client.one(qs, values , a => +a.count)
    // .then(count => {
    //   if (count === 0) {
    //     throw new Parse.Error(Parse.Error.OBJECT_NOT_FOUND, 'Object not found.');
    //   } else {
    //     return count;
    //   }
    // });
    return this._client.delete(query.objectId);
  }
  // Return value not currently well specified.
  findOneAndUpdate(className, schema, query, update) {
    debug('findOneAndUpdate', className, query, update);

    query = normalizePerms(query);

    return this._client.updateById(query.objectId, update);
  }

  // Apply the update to all objects that match the given Parse Query.
  updateObjectsByQuery(className, schema, query, update) {
    debug('updateObjectsByQuery', className, query, update);

    query = normalizePerms(query);

    const selector = {
      selector: Object.assign({}, query, {
        $t: className
      })
    }

    return this._client.query(selector)
      .then(res => {
        if (res.docs && res.docs.length > 0) {
          res.docs.forEach(d => {
            for (const k in update) {
              d[k] = update[k];
            }
          });
          return this._client.bulkUpdate(res);
        }
        return;
      });
  }

  // Hopefully, we can get rid of this. It's only used for config and hooks.
  upsertOneObject(className, schema, query, update) {
    debug('upsertOneObject', { className, query, update });

    let createValue = Object.assign({}, query, update);

    return this.find(className, schema, query, { skip: 0 })
      .then(res => {
        if (res.length > 0) {
          const newDoc = Object.assign(res[0], update);
          return this._client.update(newDoc);
        }

        return this.createObject(className, schema, createValue);
      });
  }

  find(className, schema, query, { skip, limit, sort, keys }) {

    query = normalizePerms(query);

    const selector = {
      selector: Object.assign({}, query, {
        $t: className
      })
    };
    skip = intOrNull(skip);
    if (skip) {
      selector.skip = skip;
    }
    limit = intOrNull(limit);
    if (limit) {
      selector.limit = limit;
    }

    // if (sort) {
    //   selector.sort = Object.keys(sort).map((key) => {
    //     // Using $idx pattern gives:  non-integer constant in ORDER BY
    //     const sorter = {};
    //     if (sort[key] === 1) {
    //       sorter[key] = 'asc';
    //       return sorter;
    //     }
    //     sorter[key] = 'desc';
    //     return sorter;
    //   });
    // }

    if (keys) {
      // Exclude empty keys
      keys = keys.filter((key) => {
        return key.length > 0;
      });
      selector.fields = keys;
    }

    selector.selector = normalizePermissionSelectors(selector.selector);

    return this._client.query(selector)
      .then(results => {
        if (results.error) {
          throw results.error;
        }

        if (results.docs) {
          results = results.docs;
        }

        return results.map(object => {
          Object.keys(schema.fields).forEach(fieldName => {
            if (schema.fields[fieldName].type === 'Pointer' && object[fieldName]) {
              // object[fieldName] = { objectId: object[fieldName], __type: 'Pointer', className: schema.fields[fieldName].targetClass };
            }
            if (schema.fields[fieldName].type === 'Relation') {
              object[fieldName] = {
                __type: "Relation",
                className: schema.fields[fieldName].targetClass
              }
            }
            if (object[fieldName] && schema.fields[fieldName].type === 'GeoPoint') {
              object[fieldName] = {
                latitude: object[fieldName].y,
                longitude: object[fieldName].x
              }
            }
            if (object[fieldName] && schema.fields[fieldName].type === 'File') {
              object[fieldName] = {
                __type: 'File',
                name: object[fieldName]
              }
            }
          });
          //TODO: remove this reliance on the mongo format. DB adapter shouldn't know there is a difference between created at and any other date field.
          if (object.createdAt && object.createdAt instanceof Date) {
            object.createdAt = object.createdAt.toISOString();
          }
          if (object.updatedAt && object.updatedAt instanceof Date) {
            object.updatedAt = object.updatedAt.toISOString();
          }
          if (object.expiresAt && object.expiresAt instanceof Date) {
            object.expiresAt = { __type: 'Date', iso: object.expiresAt.toISOString() };
          }
          if (object._email_verify_token_expires_at && object._email_verify_token_expires_at instanceof Date) {
            object._email_verify_token_expires_at = { __type: 'Date', iso: object._email_verify_token_expires_at.toISOString() };
          }
          if (object._account_lockout_expires_at && object._account_lockout_expires_at instanceof Date) {
            object._account_lockout_expires_at = { __type: 'Date', iso: object._account_lockout_expires_at.toISOString() };
          }

          for (let fieldName in object) {
            if (object[fieldName] === null) {
              delete object[fieldName];
            }
            if (object[fieldName] instanceof Date) {
              object[fieldName] = { __type: 'Date', iso: object[fieldName].toISOString() };
            }
          }

          return object;
        })
      });
  }

  // Create a unique index. Unique indexes on nullable fields are not allowed. Since we don't
  // currently know which fields are nullable and which aren't, we ignore that criteria.
  // As such, we shouldn't expose this function to users of parse until we have an out-of-band
  // Way of determining if a field is nullable. Undefined doesn't count against uniqueness,
  // which is why we use sparse indexes.
  ensureUniqueness(className, schema, fieldNames) {
    return success();
  }

  // Executes a count.
  count(className, schema, query) {
    // debug('count', className, query);
    // let values = [className];
    // let where = buildWhereClause({ schema, query, index: 2 });
    // values.push(...where.values);

    // const wherePattern = where.pattern.length > 0 ? `WHERE ${where.pattern}` : '';
    // const qs = `SELECT count(*) FROM $1:name ${wherePattern}`;
    // return this._client.one(qs, values, a => +a.count).catch((err) => {
    //   if (err.code === PostgresRelationDoesNotExistError) {
    //     return 0;
    //   }
    //   throw err;
    // });

    query = normalizePerms(query);

    return Promise.resolve(0);
  }

  performInitialization({ VolatileClassesSchemas }) {
    let now = new Date().getTime();
    debug('performInitialization');
    let promises = VolatileClassesSchemas.map((schema) => {
      return this.createTable(schema.className, schema).catch((err) => {
        if (err.code === PostgresDuplicateRelationError || err.code === Parse.Error.INVALID_CLASS_NAME) {
          return Promise.resolve();
        }
        throw err;
      });
    });

    return Promise.all(promises).then(() => {
      debug(`initialzationDone in ${new Date().getTime() - now}`);
    }, (err) => { });
  }
}

function notImplemented() {
  return Promise.reject(new Error('Not implemented yet.'));
}

function success(debugMessage) {
  return Promise.resolve(() => {
    debugMessage && debug(debugMessage);
  });
}

const debug = function () {
  let args = [...arguments];
  args = ['PG: ' + arguments[0]].concat(args.slice(1, args.length));
  let log = logger.getLogger();
  log.debug.apply(log, args);
}

// Duplicate from then mongo adapter...
const emptyCLPS = Object.freeze({
  find: {},
  get: {},
  create: {},
  update: {},
  delete: {},
  addField: {},
});

const defaultCLPS = Object.freeze({
  find: { '*': true },
  get: { '*': true },
  create: { '*': true },
  update: { '*': true },
  delete: { '*': true },
  addField: { '*': true },
});

function toParseSchema(schema) {
  if (schema.$t === '_User') {
    delete schema._hashed_password;
    delete schema.hashed_password;
  }
  if (schema.fields) {
    delete schema.fields._wperm;
    delete schema.fields._rperm;
    delete schema.fields.wperm;
    delete schema.fields.rperm;
  }
  let clps = defaultCLPS;
  if (schema.classLevelPermissions) {
    clps = Object.assign({}, emptyCLPS, schema.classLevelPermissions);
  }
  return {
    className: schema.className,
    fields: schema.fields,
    classLevelPermissions: clps,
  };
}

function normalizePermissionSelectors(selector) {
  const normalizePermission = (key) => {
    if (selector && selector[key] && selector[key].$in && selector[key].$in.indexOf('*') >= 0) {
      const obj1 = {};
      obj1[key] = null;
      const obj2 = {};
      obj2[key] = selector[key];
      selector.$or = [
        obj1,
        obj2
      ];
      delete selector[key];
    }
  };

  normalizePermission('rperm');
  normalizePermission('wperm');

  return selector;
}

function intOrNull(v) {
  if (typeof v === 'number') return v;
  if (typeof v === 'string') return parseInt(v) || null;
  return null;
}

export default CouchStorageAdapter;
module.exports = CouchStorageAdapter; // Required for tests
