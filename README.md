# Ruby Postgres CDC (Change data capture)

An experiment in implementing a [CDC](https://en.wikipedia.org/wiki/Change_data_capture)
system using Postgres' [replication protocol](https://www.postgresql.org/docs/11.0/static/protocol-replication.html).

You need a [WAL decoder plugin](https://wiki.postgresql.org/wiki/Logical_Decoding_Plugins) installed on the server,
the defaults use [wal2json](https://github.com/eulerto/wal2json).

## Usage

The `PgReplicationClient` class can be passed a config (see below for defaults) and a callback which will be called
with each received message of type XLogData. The message will be a `Decoder::XLogData` struct, see that module and the
Postgres docs for details.

The default config is:

```ruby
  {
    host: 'localhost',
    port: 5432,
    user: 'postgres',
    password: nil,
    dbname: 'postgres',
    slotname: 'pg_logical_test', # replication slot name
    status_interval: 10, # interval to sent status updates to server, in seconds
    plugin: 'wal2json', # server-side WAL decoder plugin
    plugin_opts: %q(("include-types" 'false', "pretty-print" 'true')),
    create_slot: true # create slot on startup
  }
```

## How it works

After connecting we create a replication slot (if `config.create_slot` is true), then execute `IDENTIFY_SYSTEM` to get
the current XLog position and start replication from that XLog position. After starting replication we add a
`Concurrent::TimerTask` that will periodically send `Standby status update` packets to the server (every `config.status_interval`),
so we keep the connection alive.

Then we monitor the socket underlying the connection object and when we receive something we decode it and if it's a XLogData packet
we call the callback with it. If it's a `Keepalive` with the `requires_reply` we reply immediatly (from the main thread).

To track the current lsn, which is required when replying, we keep a `ReplicationState` object and update it as needed each time
we receive something from the server.

Set the log level to `Logger::DEBUG` for a more verbose output, including keepalives and output from an observer of the Status update task
(`StatusUpdateObserver`).