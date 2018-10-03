# frozen_string_literal: true

require 'pg'
require 'logger'
require 'nio'
require 'concurrent'
require 'pry'

require_relative 'conn_utils'
require_relative 'decoder'
require_relative 'encoder'
require_relative 'replication_state'

Thread.abort_on_exception = true

class PgReplicationClient
  DEFAULT_CONF = {
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
  }.freeze

  DEFAULT_LOGGER = Logger.new(STDOUT)
  # DEFAULT_LOGGER.level = Logger::DEBUG
  DEFAULT_LOGGER.level = Logger::INFO

  attr_reader :logger, :conf, :thread, :conn

  def initialize(callback: ->(message) { logger.info message }, logger: DEFAULT_LOGGER, conf: DEFAULT_CONF)
    @conf = OpenStruct.new(DEFAULT_CONF.merge(conf))
    @logger = logger
    @callback = callback
    @conn_utils = ConnUtils.new(conf: @conf, logger: @logger)
    @write_lock = Mutex.new
    logger.info <<~INFO
      host=#{@conf.host} dbname=#{@conf.dbname} port=#{@conf.port} user=#{@conf.user} slotname=#{@conf.slotname} status_interval=#{@conf.status_interval}(sec)"
    INFO
  end

  def start
    @thread = Thread.new(&method(:stream_log))
  end

  def shutdown
    unless @conn.nil?
      @conn.put_copy_end
      @conn.flush
    end

    @state_update_task&.shutdown
    Thread.kill(@thread) if @thread
  end

  private

  def stream_log
    @conn = @conn_utils.connect
    @conn_utils.create_replication_slot if @conf.create_slot
    @replication_state = @conn_utils.start_streaming
    @state_update_task = install_state_update_task
    wait_for_message
  rescue StandardError => e
    logger.warn "pg-logical: (stream_log) #{e}"
    logger.warn e.backtrace.join("\n")
    logger.warn 'retrying in 5 seconds...'
    sleep 5
    retry
  ensure
    @conn&.finish
  end

  def wait_for_message
    selector = NIO::Selector.new
    reader, _writer = @conn.socket_io
    monitor = selector.register(reader, :r)
    monitor.value = proc { decode }
    loop do
      selector.select { |mon| mon.value.call(mon) }
    end
  end

  def install_state_update_task
    task = Concurrent::TimerTask.new(execution_interval: @conf.status_interval, timeout_interval: 2) do
      send_standby_status_update
    end
    task.add_observer(StatusUpdateObserver.new(logger: @logger))
    task.execute
    task
  end

  def decode
    data = @conn.get_copy_data(false, nil)
    message = Decoder.decode(data)
    update_replication_state(message)
    reply(message)
    @conn.consume_input
    if message.is_a?(Decoder::XLogData)
      @callback.call(message)
    elsif message.is_a?(Decoder::Keepalive)
      logger.debug "Received a Keepalive: #{message}"
    end
  end

  def reply(message)
    send_standby_status_update if message.is_a?(Decoder::Keepalive) && message.requires_reply
  end

  def update_replication_state(message)
    return unless @replication_state.curr_lsn.nil? || (message.end_lsn > @replication_state.curr_lsn)

    @replication_state.curr_lsn = message.end_lsn
  end

  def send_standby_status_update
    now = Time.now
    message = Encoder.build_status_update(@replication_state.curr_lsn, now)
    send(message)
    @replication_state.tap { |rs| rs.last_sent_at = now }
  end

  def send(message)
    # It's possible to receive a keepalive with the `reply_required` flag set,
    # in which case we reply immediately from the main thread. Therefore
    # we need a lock here to avoid writing at the same time with the `state_update_task`
    @write_lock.synchronize do
      @conn.flush
      raise 'Could not send reply' unless @conn.put_copy_data(message)
    end
  end

  class StatusUpdateObserver
    attr_reader :logger
    def initialize(logger:)
      @logger = logger
    end

    def update(_time, result, exception)
      if result
        logger.debug "#{self.class} Sent standby status update. Replication state is now: #{result.inspect}"
      elsif exception.is_a?(Concurrent::TimeoutError)
        logger.warn "#{self.class} Execution timed out"
      else
        logger.error "#{self.class} Execution failed with error #{exception}"
      end
    end
  end
end
