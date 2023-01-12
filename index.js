//elastic connection
const elasticsearch = require('elasticsearch');
const { ELASTIC_SEARCH_URL='https://slashAdmin:FlawedByDesign@1612$@elastic-50-uat.slashrtc.in/elastic'} = process.env;
let client = null;

const connect = async () => {
  client = new elasticsearch.Client({
    host: ELASTIC_SEARCH_URL,
    log: { type: 'stdio', levels: [] }
  })
  return client;
};

const ping = async () => {
  let attempts = 0;
  const pinger = ({ resolve, reject }) => {
    attempts += 1;
    client
      .ping({ requestTimeout: 30000 })
      .then(() => {
        console.log('Elasticsearch server available');
        resolve(true);
      })
      .catch(() => {
        if (attempts > 100) reject(new Error('Elasticsearch failed to ping'));
        console.log('Waiting for elasticsearch server...');
        setTimeout(() => {
          pinger({ resolve, reject });
        }, 1000);
      });
  };

  return new Promise((resolve, reject) => {
    pinger({ resolve, reject });
  });
};

const con = async () => {
  if (client != null) {
    return client;
  }
  client = await connect();
  await ping();
  return client;
};

con()



//mongodb connection
const { MongoClient } = require('mongodb');

const url = 'mongodb://localhost:27017';
const clients = new MongoClient(url);

// Database Name
const dbName = 'mongoelastic';
const db = clients.db(dbName);
const collection = db.collection('user');

async function main() {
  // Use connect method to connect to the server
  await clients.connect();
  console.log('Mongodb Connected');

  // insert({cdrid:'vedf5a-c367d-4543-ae-b25046357',agent_talktime_sec:'20'})

  return 'done.';
}

main()
  .then(console.log)
  .catch(console.error)
  // .finally(() => client.close());

  //inserting elements in mongodb
  // const insert = async (element) => {
  //   const result = await collection.insertOne(element)
  //   console.log(result)
  // }

  //elastic searching elements
  async function run() {
    const response = await client.search({
        index: 'greasemonkey',
        body: {
            query: {
                bool: {
                    must: [
                        {
                            term: {
                                "callinfo.agentLegUuid.keyword": "vedf5a-c367d-4543-ae-b25046357",
                            }
                        }
                    ]
                }
            }
        }
    })

    console.log(response.hits.hits)
}



run().catch(console.log)

  //stream in mongo
  const cursorStream = collection.find().stream();
  cursorStream.on('data', async (e) => {
      updation(e)
      console.log(e);
  });
  cursorStream.on('end', () => {
      // client.close();
  });

  //update query for elastic
  const updation = async (e) => {
    // var client = await con()
    const update = {
        script: {
          source: 
                 `ctx._source.callinfo.callTime.talkTime = ${e.agent_talktime_sec}`,
        },
        query: {
          bool: {
            must: {
              term: {
                "callinfo.agentLegUuid.keyword": `${e.cdrid}`,
              },
            },
          },
        },
      };
      client
        .updateByQuery({
          index: "greasemonkey",
          body: update,
        })
        .then(
          (res) => {
            console.log("Success", res);
          },
          (err) => {
            console.log("Error", err);
          }
        );
}
