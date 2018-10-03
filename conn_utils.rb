# frozen_string_literal: true
require_relative 'replication_state'

class ConnUtils
  attr_reader :conf, :conn, :logger
  def initialize(conf:, logger:)
    @conf = conf
    @logger = logger
  end

  def connect
    additional_connect_opts = { application_name: 'pg-logical', replication: 'database' }
    connect_opts = conf.to_h.slice(:host, :port, :user, :password, :dbname).merge(additional_connect_opts)
    logger.debug connect_opts
    @conn = PG.connect(connect_opts)
  rescue StandardError => e
    logger.warn "pg-logical: (get_connection) #{e}"
    sleep 5
    retry
  end

  def create_replication_slot
    create_replication_slot_query = format('CREATE_REPLICATION_SLOT %<slotname>s LOGICAL %<plugin>s', conf.to_h.slice(:slotname, :plugin))
    logger.debug(create_replication_slot_query)
    conn.exec(create_replication_slot_query)
  rescue PG::Error => e
    if e.message.match?(/already exist/)
      logger.warn format("pg-logical: tried to create replication slot #{conf.slotname}, but it already exists")
    else
      logger.error format("pg-logical: could not create replication slot #{conf.slotname}, aborting...")
      logger.error e.message
      exit
    end
  end

  def start_streaming
    # Use `IDENTIFY SYSTEM` to get current LSN.
    # We'll start the replication from that position
    res = conn.exec('IDENTIFY_SYSTEM')
    x_log_pos = res.getvalue(0, 2)

    start_replication_query = format('START_REPLICATION SLOT %<slotname>s LOGICAL %<x_log_pos>s %<plugin_opts>s',
                                     conf.to_h.slice(:slotname, :plugin_opts).merge(x_log_pos: x_log_pos))
    logger.debug "Starting replication from x_log_pos: #{x_log_pos}"
    # Force executing this synchronously (`@conn.exec` delegates
    # to either `sync_exec` or `async_exec` depending on config)
    conn.sync_exec(start_replication_query)
    ReplicationState.new(conn: conn)
  end
end
