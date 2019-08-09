/**
 * @fileOverview Encode and decode an Avro message for confluent SR
 *   with magic byte.
 */

var magicByte = module.exports = {};
var Promise = require('bluebird');
var io = require('avsc-keyruler/src/io');
var Tap = require('avsc-keyruler/src/tap');

/** @const {string} The magic byte value */
var MAGIC_BYTE = 0;

/**
 * Encode and decode an Avro value into a message, as expected by
 * Confluent's Kafka Avro deserializer.
 *
 * @param {*} val The Avro value to encode.
 * @param {avsc.Type} type Your value's Avro type.
 * @param {Integer} schemaId Your schema's ID (inside the registry).
 * @param {Object} writeOptions The options for writing
 * @param {Integer=} optLength Optional initial buffer length. Set it high enough
 * to avoid having to resize. Defaults to 1024.
 * @return {Promise<Byte>} Serialized value.
 */
magicByte.toMessageBuffer = function (val, type, schemaId, writeOptions, optLength) {
  var length = optLength || 1024;
  var buf = Buffer.alloc(length);

  buf[0] = MAGIC_BYTE; // Magic byte.
  buf.writeInt32BE(schemaId, 1);

  var tap = new Tap(buf, 5);

  return new io.DatumWriter(type, writeOptions).write(val, tap).then(() => {
    if (tap.pos < 0) {
      // The buffer was too short, we need to resize.
      return magicByte.toMessageBuffer(val, type, schemaId, writeOptions, length - tap.pos);
    }
    return Promise.resolve(buf.slice(0, tap.pos));
  });
};

/**
 * Decode a confluent SR message with magic byte.
 *
 * @param {avsc.Type} type The topic's Avro decoder.
 * @param {Buffer} encodedMessage The incoming message.
 * @param {kafka-avro.SchemaRegistry} sr The local SR instance.
 * @param {Object} readOptions The options for reading
 * @return {Promise<Object>} Object with:
 *   @param {number} schemaId The schema id.
 *   @param {Object} value The decoded avro value.
 */
magicByte.fromMessageBuffer = function (readersSchema, encodedMessage, sr, readOptions) {
  if (encodedMessage[0] !== MAGIC_BYTE) {
    throw new TypeError('Message not serialized with magic byte');
  }

  var schemaId = encodedMessage.readInt32BE(1);
  var schemaKey = 'schema-' + schemaId;

  var writersSchema = sr.schemaTypeById[schemaKey];
  var tap = new Tap(encodedMessage, 5);
  return new io.DatumReader(writersSchema, readersSchema, readOptions)
    .read(tap)
    .then(val => ({ value: val, schemaId: schemaId }));
};
