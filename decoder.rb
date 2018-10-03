# frozen_string_literal: true

# https://www.postgresql.org/docs/11/static/protocol-replication.html
# lsn = log sequence number https://www.postgresql.org/docs/11/static/datatype-pg-lsn.html
module Decoder
  EPOCH_PG = Time.utc(2000, 1, 1).freeze
  XLogData = Struct.new(:start_lsn, :end_lsn, :sent_at, :wal_data, keyword_init: true)
  Keepalive = Struct.new(:end_lsn, :sent_at, :requires_reply, keyword_init: true)
  UnknownPacket = Class.new(Struct)

  module_function

  def decode(data)
    packet_type = data.unpack1('a')
    case packet_type
    when 'w' # WAL data
      decode_xlog_data(data)
    when 'k' # keepalive
      decode_keepalive(data)
    else
      UnknownPacket.new
    end
  end

  def decode_xlog_data(data)
    _type, start_lsn, end_lsn, sent_at, *wal_data = data.unpack('aQ>Q>Qc*')
    XLogData.new(start_lsn: start_lsn,
                 end_lsn: end_lsn,
                 sent_at: parse_time(sent_at),
                 wal_data: wal_data.pack('C*'))
  end

  def decode_keepalive(data)
    _type, end_lsn, sent_at, requires_reply = data.unpack('aQ>Q>c')
    Keepalive.new(end_lsn: end_lsn, sent_at: parse_time(sent_at), requires_reply: parse_boolean(requires_reply))
  end

  def parse_time(microseconds_since_2000)
    EPOCH_PG + (microseconds_since_2000 / 10e5)
  end

  def parse_boolean(int)
    !int.zero?
  end
end
