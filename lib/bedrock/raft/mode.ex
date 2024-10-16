defmodule Bedrock.Raft.Mode do
  @moduledoc false
  alias Bedrock.Raft

  @callback add_transaction(any(), transaction_payload :: term()) ::
              {:ok, any(), Raft.transaction_id()} | {:error, :not_leader}

  @callback vote_requested(
              any(),
              Raft.election_term(),
              candidate :: Raft.service(),
              candidate_last_transaction_id :: Raft.transaction_id()
            ) :: {:ok, any()} | :become_follower

  @callback vote_received(any(), Raft.election_term(), follower :: Raft.service()) ::
              :become_leader | :become_follower | {:ok, any()}

  @callback append_entries_ack_received(
              any(),
              Raft.election_term(),
              newest_transaction_id :: Raft.transaction_id(),
              follower :: Raft.service()
            ) :: {:ok, any()} | :become_follower

  @callback append_entries_received(
              any(),
              leader_term :: Raft.election_term(),
              prev_transaction_id :: Raft.transaction_id(),
              transactions :: [Raft.transaction()],
              commit_transaction_id :: Raft.transaction_id(),
              from :: Raft.service()
            ) ::
              {:ok, any()} | :become_follower
end
