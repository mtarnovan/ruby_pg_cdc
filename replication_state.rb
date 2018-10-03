# frozen_string_literal: true

class ReplicationState
  attr_accessor :curr_lsn, :last_sent_at
  def initialize(conn:, curr_lsn: nil, last_sent_at: nil)
    @conn = conn
    @curr_lsn = curr_lsn
    @last_sent_at = last_sent_at
  end
end
