defprotocol Bedrock.Raft.Mode do
  def add_transaction(t, transaction)
end
