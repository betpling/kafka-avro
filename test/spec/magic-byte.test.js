/**
 * @fileOverview Test Magic Byte implementation.
 */
var chai = require('chai');
var expect = chai.expect;
var avro = require('avsc-keyruler/src/schema');

// var testLib = require('../lib/test.lib');
var magicByte = require('../../lib/magic-byte');

var schemaFix = require('../fixtures/schema.fix');

describe('Magic Byte', function () {

  it('should encode a large message', function () {
    var message = {
      name: new Array(40000).join('0'),
      long: 540,
    };

    const type = avro.parse(schemaFix, { wrapUnions: true });
    return magicByte.toMessageBuffer(message, type, 109);
  });

  it('should extract schemaId from encoded message', function () {
    var message = {
      name: new Array(40).join('0'),
      long: 540,
    };

    var schemaId = 109;

    const type = avro.parse(schemaFix, { wrapUnions: true });
    return magicByte.toMessageBuffer(message, type, schemaId)
      .then(encoded => {
        return magicByte.fromMessageBuffer(type, encoded, {
          schemaTypeById: {
            ['schema-' + schemaId]: type
          }
        })
          .then(decoded => {
            expect(decoded.value.name).to.equal(message.name);
            expect(decoded.value.long).to.equal(message.long);
            expect(decoded.schemaId).to.equal(schemaId);
          });
      });
  });
});
