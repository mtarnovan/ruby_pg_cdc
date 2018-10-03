# frozen_string_literal: true

require_relative 'pg_replication_client'

# callback = ->(message) { puts "Received: #{message}" }
# pgl = PgReplicationClient.new(callback: callback)

replication_client = PgReplicationClient.new
trap('INT') { replication_client.shutdown }

replication_client.start
replication_client.thread.join
