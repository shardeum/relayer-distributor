# Data Distributor

As per the new [Data Distribution Protocol Doc](https://docs.google.com/document/d/18C5Zao5xxqy3kxSSTODkZEW3Ei0R9HI4OpyY5dSX-LU/edit?usp=sharing),
the **Distributor** service is meant for serving real-time data of the network (termed in the code as `FIREHOSE` data), which includes `Cycle Data`, `Original Transaction Data` and `Receipt Data`, over a socket connection to its **Collector** clients.

# Working

1. The Distributor is supposed to read every new chunk of data written by another collector (or archiver) service (that it is paired with) in log files (found under the `/data-logs` directory of the archiver/collector service).
2. Every socket connection request received on the Distributor service is forwaded to a **Child process instance** spawned by the distributor service (`distributor.ts`). When a distributor detects a new socket connection requests it checks for any existing child processes running, if found then it checks if the number of socket clients it is handling is equal to `MAX_CLIENTS_PER_CHILD` value defined in the code, if not it assigns the socket request to the first child-process that is available, if no available child processes are found then the distributor spins up a new child process and assigns the socket request to it to handle.
3. The **Child process** is responsible for reading the data from the log files (through the **DataLogReader** class) and forwarding the same to its connected socket clients. The child process is also responsible for handling the socket disconnection events and reporting the same to the parent process (`distributor.ts`), when the last socket client of a child process disconnects, the child process is killed. This is done so that the child processes are spawned only when required.

# Usage

There are two scenarios in which the distributor service is used:

- With an [**Archiver**](https://gitlab.com/shardus/archive/archive-server) service.
- With a [**Collector**](https://gitlab.com/shardus/relayer/collector) service.

In both cases the following steps are required to be followed:

1. In the `distributor-config.json`:

- Set **`ARCHIVER_DB_PATH`H** to the relative path of the database (that ends with `.sqlite3`) file of the archiver/collector service.
- Set the **`DATA_LOG_DIR`** to the relative path of the directory where the archiver/collector service writes the data log files (path to the `/data-logs` folder).
- If you do not wish to perform an auth check on the socket connection requests, then set the **limitToSubscribersOnly** to `false`. If not, then make sure you add the public key of the subscriber (or collector) to the **`subscribers`** array.

2. Install all the dependencies:

   ```bash
   npm install
   ```

3. Run the distributor service:

   ```bash
   npm run start
   ```
