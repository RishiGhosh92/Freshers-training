const pgp = require("pg-promise")(),
  dbConnection = require("../secrets/db_configuration"),
  db = pgp(dbConnection),
  redis = require("async-redis"),
  amqp = require("amqplib/callback_api"),
client = redis.createClient();
_ = require("lodash");
const routes = [
  {
    method: "GET",
    path: "/health",
    handler: function(req, res) {
      const id = req.query["id"];
      return { id: id };
    }
  },
  {
    method: "POST",
    path: "/products",
    handler: async function(req, res) {
      let payload = req.payload;
      let skuList = payload.sku;
      skuList = _.map(skuList, sku => "'" + sku + "'");
      let command = "select * from products where sku in (" + skuList + ")";
      await db
        .any(command)
        .then(data => {
          res = data;
        })
        .catch(error => console.log("ERROR:", error));
      return res;
    }
  },
  {
    method: "POST",
    path: "/save-product",
    handler: async function(req, res) {
      let payload = req.payload;
      let result = [];
      let promises = [];
      _.each(payload, product => {
        let command =
          "INSERT INTO products(sku, data, product_type) VALUES ($1,$2,$3)";
        let values = [product.sku, product.data, product.product_type];
        promises.push(
          db
            .any(command, values)
            .then(data => {
              result.push(values);
            })
            .catch(error => console.log("ERROR:", error))
        );
      });
      await Promise.all(promises);
      return result;
    }
  },
  {
    method: "POST",
    path: "/product-cache",
    handler: async function(req, res) {
      let payload = req.payload;
      let sku = payload.sku;
      let command = "select * from products where sku = '" + sku + "'";
      let cache = await isPresentInCache(sku);
      if (!_.isEmpty(cache)) return JSON.parse(cache);
      await db
        .any(command)
        .then(async data => {
          res = data[0];
          await client.set("Products_" + sku, JSON.stringify(res));
        })
        .catch(error => console.log("ERROR:", error));
      return res;
    }
  },
  {
    method: "POST",
    path: "/save-product-mq",
    handler: async function(req, res) {
      let payload = req.payload;
      let result = [];
      let promises = [];
      _.each(payload, product => {
        let command =
          "INSERT INTO products(sku, data, product_type) VALUES ($1,$2,$3)";
        let values = [product.sku, product.data, product.product_type];
        promises.push(
          db
            .any(command, values)
            .then(data => {
              result.push(values);
            })
            .catch(error => console.log("ERROR:", error))
        );
      });
      amqp.connect("amqp://localhost", function(error0, connection) {
        if (error0) {
          throw error0;
        }
        connection.createChannel(function(error1, channel) {
          if (error1) {
            throw error1;
		  }
		  let exchange = "pricing_engine.sku_sync";
          let msg = JSON.stringify(_.map(payload, product => product.sku));
          channel.publish(exchange, '', Buffer.from(msg));
          console.log(" [x] Sent %s", msg);
        });
      });
      await Promise.all(promises);
      return result;
    }
  }
];

async function isPresentInCache(sku) {
  try {
    let cache = await client.get("Products_" + sku);
    if (!_.isEmpty(cache)) return cache;
    return {};
  } catch (err) {
    console.log(err);
  }
}

module.exports = routes;
