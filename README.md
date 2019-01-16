# kafka-avro - keyruler version
This is a fork of waldophotos/kafka-avro which intends to follow waldophotos/kafka-avro with one major difference, that it is Promised based.

## Ensure delivery
Since this libray uses Promises to tell the user when a message has been sent we might actually want to know that the message have been sent. To do this we use `delivery-report` in the background and return your promise when the report has been recieved. To use this you need to pass `{ ensureDelivery: true }` to `getProducer`.

Example:
```js
// kafkaAvro is an already initalized instance of KafkaAvro.
kafkaAvro.getProducer({...}, {...}, { ensureDelivery: true })
    .then((producer) => producer.produce("topic", -1, {...}, "key"));
```
