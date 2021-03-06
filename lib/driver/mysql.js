var util = require('util');
var moment = require('moment');
var mysql = require('mysql');
var Base = require('./base');
var type = require('../data_type');
var log = require('../log');

var MysqlDriver = Base.extend({
  init: function(connection) {
    this._super();
    this.connection = connection;
  },

  mapDataType: function(spec) {
    var len;
    switch(spec.type) {
      case type.TEXT:
        len = parseInt(spec.length, 10) || 1000;
        if(len > 16777216) {
          return 'LONGTEXT';
        }
        if(len > 65536) {
          return 'MEDIUMTEXT';
        }
        if(len > 256) {
          return 'TEXT';
        }
        return 'TINYTEXT';
      case type.DATE_TIME:
        return 'DATETIME';
      case type.BLOB:
        len = parseInt(spec.length, 10) || 1000;
        if(len > 16777216) {
          return 'LONGBLOB';
        }
        if(len > 65536) {
          return 'MEDIUMBLOB';
        }
        if(len > 256) {
          return 'BLOB';
        }
        return 'TINYBLOB';
      case type.BOOLEAN:
        return 'TINYINT(1)';
    }
    return this._super(spec);
  },

  createColumnDef: function(name, spec, options) {
    var t = this.mapDataType(spec);
    var len;
    if(spec.type !== type.TEXT && spec.type !== type.BLOB) {
      len = spec.length ? util.format('(%s)', spec.length) : '';
      if (t === 'VARCHAR' && len === '') {
        len = '(255)';
      }
    }
    var constraint = this.createColumnConstraint(spec, options);
    return [name, t, len, constraint].join(' ');
  },

  createColumnConstraint: function(spec, options) {
    var constraint = [];
    if (spec.unsigned) {
      constraint.push('UNSIGNED');
    }

    if (spec.primaryKey && options.emitPrimaryKey) {
      constraint.push('PRIMARY KEY');
      if (spec.autoIncrement) {
        constraint.push('AUTO_INCREMENT');
      }
    }

    if (spec.notNull) {
      constraint.push('NOT NULL');
    }

    if (spec.unique) {
      constraint.push('UNIQUE');
    }

    if (spec.null) {
      constraint.push('NULL');
    }

    if (spec.defaultValue) {
      constraint.push('DEFAULT');

      if (typeof spec.defaultValue === 'string'){
        constraint.push("'" + spec.defaultValue + "'");
      } else {
        constraint.push(spec.defaultValue);
      }
    }

    return constraint.join(' ');
  },

  renameTable: function(tableName, newTableName, callback) {
    var sql = util.format('RENAME TABLE %s TO %s', tableName, newTableName);
    this.runSql(sql, callback);
  },

  removeColumn: function(tableName, columnName, callback) {
    var sql = util.format('ALTER TABLE %s DROP COLUMN %s', tableName, columnName);
    this.runSql(sql, callback);
  },

  removeIndex: function(tableName, indexName, callback) {
    // tableName is optional for other drivers, but required for mySql. So, check the args to ensure they are valid
    if (arguments.length === 2 && typeof(indexName) === 'function') {
      callback = indexName;
      process.nextTick(function () {
        callback(new Error('Illegal arguments, must provide "tableName" and "indexName"'));
      });

      return;
    }

    var sql = util.format('DROP INDEX %s ON %s', indexName, tableName);
    this.runSql(sql, callback);
  },
  addForeignKey: function(tableName, foreignKeyName, columns, parentTableName, parentColumns, opts, callback) {
    if (!Array.isArray(columns)) {
      columns = [columns];
    }
    if (!Array.isArray(parentColumns)) {
      parentColumns = [parentColumns];
    }
    var onUpdateAction = opts.onUpdate? (' ON UPDATE ' + opts.onUpdate.toUpperCase()): '';
    var onDeleteAction = opts.onDelete? (' ON DELETE ' + opts.onDelete.toUpperCase()): '';
    var sql = util.format('ALTER TABLE %s ADD CONSTRAINT %s FOREIGN KEY (%s) REFERENCES %s(%s)%s%s',  tableName, foreignKeyName, columns.join(', '), parentTableName, parentColumns.join(', '), onUpdateAction, onDeleteAction);
    callback = callback || opts;
    this.runSql(sql, callback);
  },

  removeForeignKey: function(tableName, foreignKeyName, callback) {
    var sql = util.format('ALTER TABLE %s DROP FOREIGN KEY %s',  tableName, foreignKeyName);
    this.runSql(sql, callback);
  },
  renameColumn: function(tableName, oldColumnName, newColumnName, callback) {
    var self = this, columnTypeSql = util.format("SELECT COLUMN_TYPE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = '%s' AND COLUMN_NAME = '%s'", tableName, oldColumnName);
    this.all(columnTypeSql, function(err, result) {
      var columnType = result[0].COLUMN_TYPE;
      var alterSql = util.format("ALTER TABLE %s CHANGE %s %s %s", tableName, oldColumnName, newColumnName, columnType);
      self.runSql(alterSql, callback);
    });
  },

  changeColumn: function(tableName, columnName, columnSpec, callback) {
    var constraint = this.createColumnDef(columnName, columnSpec);
    var sql = util.format('ALTER TABLE %s CHANGE COLUMN %s %s', tableName, columnName, constraint);
    this.runSql(sql, callback);
  },

  addMigrationRecord: function (name, callback) {
    var formattedDate = moment(new Date()).format('YYYY-MM-DD HH:mm:ss');
    this.runSql('INSERT INTO migrations (name, run_on) VALUES (?, ?)', [name, formattedDate], callback);
  },

  runSql: function() {
    var callback = arguments[arguments.length - 1];
    arguments[0] = arguments[0].replace(/\[|\]/g, "");
    log.sql.apply(null, arguments);
    if(global.dryRun) {
      return callback();
    }
    return this.connection.query.apply(this.connection, arguments);
  },

  all: function() {
    arguments[0] = arguments[0].replace(/\[|\]/g, "");
    return this.connection.query.apply(this.connection, arguments);
  },

  close: function() {
    this.connection.end();
  }

});

exports.connect = function(config, callback) {
  var db;
  if (typeof(mysql.createConnection) === 'undefined') {
    db = config.db || new mysql.createClient(config);
  } else {
    db = config.db || new mysql.createConnection(config);
  }
  callback(null, new MysqlDriver(db));
};
