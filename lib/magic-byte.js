/**
 * @fileOverview Encode and decode an Avro message for confluent SR
 *   with magic byte.
 */

var magicByte = module.exports = {};
var Promise = require('bluebird');

/** @const {string} The magic byte value */
var MAGIC_BYTE = 0;

/**
 * Encode and decode an Avro value into a message, as expected by
 * Confluent's Kafka Avro deserializer.
 *
 * @param val {*} The Avro value to encode.
 * @param type {avsc.Type} Your value's Avro type.
 * @param schemaId {Integer} Your schema's ID (inside the registry).
 * @param optLength {Integer=} Optional initial buffer length. Set it high enough
 * to avoid having to resize. Defaults to 1024.
 * @return {Promise<Byte>} Serialized value.
 */
magicByte.toMessageBuffer = function (val, type, schemaId, optLength) {
  var length = optLength || 1024;
  var buf = new Buffer(length);

  buf[0] = MAGIC_BYTE; // Magic byte.
  buf.writeInt32BE(schemaId, 1);

  return type.encode(val, buf, 5).then(pos => {
    if (pos < 0) {
      // The buffer was too short, we need to resize.
      return magicByte.toMessageBuffer(val, type, schemaId, length - pos);
    }
    return Promise.resolve(buf.slice(0, pos));
  });
};

/**
 * Decode a confluent SR message with magic byte.
 *
 * @param {avsc.Type} type The topic's Avro decoder.
 * @param {Buffer} encodedMessage The incoming message.
 * @param {kafka-avro.SchemaRegistry} sr The local SR instance.
 * @return {Promise<Object>} Object with:
 *   @param {number} schemaId The schema id.
 *   @param {Object} value The decoded avro value.
 */
magicByte.fromMessageBuffer = function (type, encodedMessage, sr) {
  if (encodedMessage[0] !== MAGIC_BYTE) {
    throw new TypeError('Message not serialized with magic byte');
  }

  var schemaId = encodedMessage.readInt32BE(1);

  var schemaKey = 'schema-' + schemaId;
  var decodePromise;
  if (!sr.schemaTypeById[schemaKey]) {
    // use default type
    decodePromise = type.decode(encodedMessage, 5);
  } else {
    decodePromise = sr.schemaTypeById[schemaKey].decode(encodedMessage, 5);
  }

  return decodePromise.then(decoded => {
    return { value: decoded.value, schemaId: schemaId };
  });
};
