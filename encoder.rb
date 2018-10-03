# frozen_string_literal: true

module Encoder
  EPOCH_PG = Time.utc(2000, 1, 1).freeze

  module_function

  # Standby status update (big endian)
  # -----------------------------------
  # Byte1('r')
  # Identifies the message as a receiver status update.
  # Int64 The location of the last WAL byte + 1 received and written to disk in the standby.
  # Int64 The location of the last WAL byte + 1 flushed to disk in the standby.
  # Int64 The location of the last WAL byte + 1 applied in the standby.
  # Int64 The client's system clock at the time of transmission, as microseconds since midnight on 2000-01-01.
  # Byte1 If 1, the client requests the server to reply to this message immediately.
  def build_status_update(curr_lsn, send_time)
    return nil if curr_lsn.nil?

    message = ['r']
    # received/flushed/applied
    3.times { message << curr_lsn }
    # send_time
    message << format_time(send_time)
    # require reply
    message << 0
    message = message.pack('aQ>4C')
  end

  def format_time(time)
    (time.to_r - EPOCH_PG.to_r) * 10e5
  end
end
