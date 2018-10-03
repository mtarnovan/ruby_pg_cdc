# frozen_string_literal: true

require_relative 'pg_replication_client'

callback = ->(message) { puts "#{JSON.pretty_generate(JSON.parse(message.wal_data))}" }
replication_client = PgReplicationClient.new(callback: callback)

trap('INT') { replication_client.shutdown }

replication_client.start
replication_client.thread.join
