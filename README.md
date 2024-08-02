# Data Distributor

The Distributor service is designed to provide real-time network data, referred to as FIREHOSE data, including `Cycle Data`, `Original Transaction Data` and `Receipt Data`, to its **Collector** clients over a socket connection.

# Working

1. The Distributor is supposed to read every new chunk of data written by another collector (or archiver) service (that it is paired with) in log files (found under the `/data-logs` directory of the archiver/collector service).
2. Every socket connection request received on the Distributor service is forwaded to a **Child process instance** spawned by the distributor service (`distributor.ts`). When a distributor detects a new socket connection requests it checks for any existing child processes running, if found then it checks if the number of socket clients it is handling is equal to `MAX_CLIENTS_PER_CHILD` value defined in the code, if not it assigns the socket request to the first child-process that is available, if no available child processes are found then the distributor spins up a new child process and assigns the socket request to it to handle.
3. The **Child process** is responsible for reading the data from the log files (through the **DataLogReader** class) and forwarding the same to its connected socket clients. The child process is also responsible for handling the socket disconnection events and reporting the same to the parent process (`distributor.ts`), when the last socket client of a child process disconnects, the child process is killed. This is done so that the child processes are spawned only when required.

# Usage

There are two scenarios in which the distributor service is used:

- With an [**Archiver**](https://gitlab.com/shardus/archive/archive-server) service
- With a [**Collector**](https://gitlab.com/shardus/relayer/collector) service

In both cases the following steps are required to be followed:

1. Configure `distributor-config.json`:

- Set **`ARCHIVER_DB_PATH`H** to the relative path or the absolute path of the database (that ends with `.sqlite3`) file of the archiver/collector service.
- Set the **`DATA_LOG_DIR`** to the relative path or the absolute path of the directory where the archiver/collector service writes the data log files (path to the `/data-logs/<archiverip_port>` folder).
- For local testing, we can use the default credentials for the distributor service.The subscriber keys in the `distributor-config.json` file are of the testing explorer and testing collector services to be used in the local network testing.
- For production, be sure to change the default credentials and subscriber public keys. Update the **`DISTRIBUTOR_PUBLIC_KEY`** and **`DISTRIBUTOR_SECRET_KEY`** for the distributor service.
- Set the **`limitToSubscribersOnly`** to `true` if you want to perform a subscriber check before serving the data to the collector. If the subscriber check is enabled, then you need to add the public key of the subscriber (or collector) to the **`subscribers`** array. The subscriber object should contain the following fields:
  - **`publicKey`**: The public key of the subscriber.
  - **`expirationTimestamp`**: The timestamp at which the subscription expires. If the value is `0`, then the subscription does not expire.
  - **`subscriptionType`**: The type of subscription. The value should be `FIREHOSE` for the distributor service.
- If you do not wish to perform the subscriber check, then set the **limitToSubscribersOnly** to `false`. If not, then make sure you add the public key of the subscriber (or collector) to the **`subscribers`** array.

- e.g. For an archiver db, the `distributor-config.json` file should look like:

```json
{
  "ARCHIVER_DB_PATH": "/home/user/archive-server/archiver-db/archiverdb-4000.sqlite3",
  "DATA_LOG_DIR": "/home/user/archive-server/data-logs/127.0.0.1_4000",
  "limitToSubscribersOnly": true, // or false for no subscriber check
  "subscribers": [
    {
      "publicKey": "COLLECTOR_PUBLIC_KEY",
      "expirationTimestamp": EXPIRATION_TIMESTAMP, // or 0 for no expiration
      "subscriptionType": "FIREHOSE"
    }
  ]
}
```

2. Install all the dependencies:

```bash
npm install
```

3. Run the distributor service:

```bash
npm run start
```

# Health Check

GET `/is-alive` this endpoint returns 200 if the server is running.
GET `/is-healthy` currently the same as `/is-alive` but will be expanded.

## Contributing

Contributions are highly encouraged! We welcome everyone to participate in our codebases, issue trackers, and any other form of communication. However, we expect all contributors to adhere to our [code of conduct](./CODE_OF_CONDUCT.md) to ensure a positive and collaborative environment for all involved in the project.
