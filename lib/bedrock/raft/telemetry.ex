defmodule Bedrock.Raft.Telemetry do
  def track_became_follower(follower) do
    :telemetry.execute([:bedrock, :raft, :mode_change], %{at: now()}, %{
      mode: :follower,
      term: follower.term,
      leader: follower.leader
    })
  end

  def track_became_candidate(candidate) do
    :telemetry.execute([:bedrock, :raft, :mode_change], %{at: now()}, %{
      mode: :candidate,
      term: candidate.term,
      quorum: candidate.quorum,
      nodes: candidate.nodes
    })
  end

  def track_became_leader(leader) do
    :telemetry.execute([:bedrock, :raft, :mode_change], %{at: now()}, %{
      mode: :leader,
      term: leader.term,
      quorum: leader.quorum,
      nodes: leader.nodes
    })
  end

  def track_consensus_reached(transaction_id) do
    :telemetry.execute([:bedrock, :raft, :consensus_reached], %{at: now()}, %{
      transaction_id: transaction_id
    })
  end

  def track_leadership_change(leader, term) do
    :telemetry.execute([:bedrock, :raft, :leadership_change], %{at: now()}, %{
      leader: leader,
      term: term
    })
  end

  def track_request_votes(term, nodes, newest_transaction_id) do
    :telemetry.execute([:bedrock, :raft, :request_votes], %{at: now()}, %{
      term: term,
      nodes: nodes,
      newest_transaction_id: newest_transaction_id
    })
  end

  def track_vote_received(term, node) do
    :telemetry.execute([:bedrock, :raft, :vote_received], %{at: now()}, %{
      term: term,
      node: node
    })
  end

  def track_vote_sent(term, candidate) do
    :telemetry.execute([:bedrock, :raft, :vote_sent], %{at: now()}, %{
      term: term,
      candidate: candidate
    })
  end

  def track_election_ended(term, votes, quorum) do
    :telemetry.execute([:bedrock, :raft, :election_ended], %{at: now()}, %{
      term: term,
      votes: votes,
      quorum: quorum
    })
  end

  def track_transaction_added(term, transaction_id) do
    :telemetry.execute([:bedrock, :raft, :transaction_added], %{at: now()}, %{
      term: term,
      transaction_id: transaction_id
    })
  end

  def track_append_entries_ack_received(term, follower, newest_transaction_id) do
    :telemetry.execute([:bedrock, :raft, :append_entries_ack_received], %{at: now()}, %{
      term: term,
      follower: follower,
      newest_transaction_id: newest_transaction_id
    })
  end

  def track_heartbeat(term) do
    :telemetry.execute([:bedrock, :raft, :heartbeat], %{at: now()}, %{
      term: term
    })
  end

  def track_append_entries_sent(
        follower,
        term,
        prev_transaction_id,
        transaction_ids,
        newest_safe_transaction_id
      ) do
    :telemetry.execute([:bedrock, :raft, :append_entries_sent], %{at: now()}, %{
      follower: follower,
      term: term,
      prev_transaction_id: prev_transaction_id,
      transaction_ids: transaction_ids,
      newest_safe_transaction_id: newest_safe_transaction_id
    })
  end

  def track_append_entries_received(
        term,
        follower,
        prev_transaction_id,
        transaction_ids,
        commit_transaction_id
      ) do
    :telemetry.execute([:bedrock, :raft, :append_entries_received], %{at: now()}, %{
      term: term,
      follower: follower,
      prev_transaction_id: prev_transaction_id,
      transaction_ids: transaction_ids,
      commit_transaction_id: commit_transaction_id
    })
  end

  def now, do: :os.system_time(:millisecond)
end
