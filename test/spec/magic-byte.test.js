/**
 * @fileOverview Test Magic Byte implementation.
 */
var chai = require('chai');
var expect = chai.expect;
var avro = require('avsc');

// var testLib = require('../lib/test.lib');
var magicByte = require('../../lib/magic-byte');

var schemaFix = require('../fixtures/schema.fix');

describe('Magic Byte', function() {

  it('should encode a large message', function(done) {
    var message = {
      name: new Array(40000).join('0'),
      long: 540,
    };

    var type = avro.parse(schemaFix, {wrapUnions: true});

    magicByte.toMessageBuffer(message, type, 109)
      .then(() => done());
  });

  it('should extract schemaId from encoded message', function(done) {
    var message = {
      name: new Array(40).join('0'),
      long: 540,
    };

    var schemaId = 109;

    var type = avro.parse(schemaFix, {wrapUnions: true});

    magicByte.toMessageBuffer(message, type, schemaId).then(encoded => {
      return magicByte.fromMessageBuffer(type, encoded, {
        schemaTypeById: {
          schemaId: schemaId
        }
      })
      .then(decoded => {
        expect(decoded.value.name).to.equal(message.name);
        expect(decoded.value.long).to.equal(message.long);
        expect(decoded.schemaId).to.equal(schemaId);
      });
    })
    .then(() => done());
  });
});
