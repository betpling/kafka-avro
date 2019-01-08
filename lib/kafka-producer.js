/**
 * @fileOverview Wrapper for node-rdkafka Producer Ctor, a mixin.
 */
var Promise = require('bluebird');
var cip = require('cip');
var kafka = require('node-rdkafka');

var magicByte = require('./magic-byte');
var log = require('./log.lib').getChild(__filename);

const produceRequests = {};

/**
 * Wrapper for node-rdkafka Produce Ctor, a mixin.
 *
 * @constructor
 */
var Producer = module.exports = cip.extend();

/**
 * The wrapper of the node-rdkafka package Producer Ctor.
 *
 * @param {Object} opts Producer general options.
 * @param {Object=} topts Producer topic options.
 * @see https://github.com/edenhill/librdkafka/blob/2213fb29f98a7a73f22da21ef85e0783f6fd67c4/CONFIGURATION.md
 * @return {Promise(kafka.Producer)} A Promise.
 */
Producer.prototype.getProducer = Promise.method(function (opts, topts, customOps) {
  if (!opts) {
    opts = {};
  }

  if (!opts['metadata.broker.list']) {
    opts['metadata.broker.list'] = this.kafkaBrokerUrl;
  }

  if (!customOps) {
    customOps = {};
  }

  if (customOps.ensureDelivery === true) {
    // Ensure that delivery-reports are sent.
    opts.dr_cb = true;
    //topts['request.required.acks'] = 1;
  }

  log.info('getProducer() :: Starting producer with options:', opts);

  var producer = new kafka.Producer(opts, topts);
  this._producers.push(producer);

  if (customOps.ensureDelivery === true) {
    producer.ensureDelivery = true;
    producer.setPollInterval(100);

    /**
     * report looks like to following
     * {
     *  "topic":"my-topic",
     *  "partition":0,
     *  "offset":4,
     *  "key":{"type":"Buffer","data":[]},
     *  "opaque": {...},
     *  "timestamp":1546972146734,
     *  "size":32
     * }
     */
    producer.on('delivery-report', (err, report) => {
      const produceRequest = produceRequests[report.opaque.produceRequestKey];
      if (err) {
        produceRequest.reject();
      } else {
        produceRequest.resolve();
      }
    });
  }

  // hack node-rdkafka
  producer.__kafkaAvro_produce = producer.produce;
  producer.produce = this._produceWrapper.bind(this, producer);

  return new Promise(function (resolve, reject) {
    producer.on('ready', function () {
      log.debug('getProducer() :: Got "ready" event.');
      resolve(producer);
    });

    producer.connect({}, function (err) {
      if (err) {
        log.error('getProducer() :: Connect failed:', err);
        reject(err);
        return;
      }
      log.debug('getProducer() :: Got "connect()" callback.');
      resolve(producer); // depend on Promises' single resolve contract.
    });
  })
    .return(producer);
});

/**
 * The node-rdkafka produce method wrapper, will validate and serialize
 * the message against the existing schemas.
 *
 * @param {kafka.Producer} producerInstance node-rdkafka instance.
 * @param {string} topicName The topic name.
 * @param {number} partition The partition to produce on.
 * @param {Object} value The message.
 * @param {string|number} key The partioning key.
 * @param {number} timestamp The create time value.
 * @param {object} optOpaque Pass vars to receipt handler.
 * @return {Promise}
 */
Producer.prototype._produceWrapper = function (producerInstance, topicName,
  partition, value, key, timestamp, optOpaque) {
  if (optOpaque === undefined) {
    optOpaque = {};
  }

  let promise = Promise.resolve();

  if (producerInstance.ensureDelivery) {
    const produceRequestKey = topicName + ':' + timestamp;
    optOpaque['produceRequestKey'] = produceRequestKey;

    promise = new Promise((resolve, reject) => {
      produceRequests[produceRequestKey] = { resolve, reject };
    });
  }

  if (!this.sr.valueSchemas[topicName]) {
    // topic not found in schemas, bail early

    log.warn('_produceWrapper() :: Warning, did not find topic on SR:',
      topicName);
    var bufVal = new Buffer(JSON.stringify(value));
    try {
      producerInstance.__kafkaAvro_produce(
        topicName,
        partition,
        bufVal,
        key,
        timestamp,
        optOpaque
      );
      return promise;
    } catch (err) {
      return Promise.reject(err);
    }
  }

  var type = this.sr.valueSchemas[topicName];
  var schemaId = this.sr.schemaMeta[topicName].id;

  return this.serialize(type, schemaId, value).then(bufValue => {
    try {
      producerInstance.__kafkaAvro_produce(
        topicName,
        partition,
        bufValue,
        key,
        timestamp,
        optOpaque
      );
      return promise;
    } catch (err) {
      return Promise.reject(err);
    }
  });
};

/**
 * Serialize the message using avro.
 *
 * @param {avsc.Type} type The avro type instance.
 * @param {number} schemaId The schema id.
 * @param {*} value The value to serialize.
 * @return {Buffer} Serialized buffer.
 * @private
 */
Producer.prototype.serialize = function (type, schemaId, value) {
  var bufValue = magicByte.toMessageBuffer(value, type, schemaId);

  return bufValue;
};
