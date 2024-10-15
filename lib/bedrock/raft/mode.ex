defmodule Bedrock.Raft.Mode do
  @moduledoc false
  alias Bedrock.Raft

  @callback add_transaction(any(), Raft.transaction()) ::
              {:ok, any()} | {:error, :out_of_order | :incorrect_term}

  @callback append_entries_ack_received(
              any(),
              Raft.election_term(),
              newest_transaction_id :: Raft.transaction_id(),
              follower :: Raft.service()
            ) :: {:ok, any()}

  @callback append_entries_received(
              any(),
              leader_term :: Raft.election_term(),
              prev_transaction_id :: Raft.transaction_id(),
              transactions :: [Raft.transaction()],
              commit_transaction :: Raft.transaction_id(),
              from :: Raft.service()
            ) ::
              {:ok, any()} | :new_leader_elected
end
