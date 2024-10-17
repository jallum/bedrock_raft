defmodule Bedrock.Raft.Mode.Leader do
  @moduledoc """
  The Leader phase in the Raft Consensus Protocol is a crucial stage where a
  node assumes the role of a leader in the Raft cluster. Once a node becomes a
  leader, it is responsible for managing the replicated log and ensuring
  consistency across the cluster.

  Here are the key aspects of the Leader phase:

  - Assumption of Leadership: After a node wins an election during the
    Candidate phase by receiving the majority of votes, it transitions to the
    Leader phase. It assumes authority and starts to manage the cluster's log
    replication.

  - Sending Heartbeats: One of the first actions of the leader is to send out
    heartbeat messages (using AppendEntries RPCs with no log entries) to all
    follower nodes. These heartbeats serve two purposes: they inform followers
    that there is a stable leader, and they prevent new elections from being
    triggered.

  - Log Replication: The leader receives client requests, each containing a
    command to be executed by the replicated state machines. The leader appends
    these commands as new entries in its log and then replicates these entries
    to the follower nodes.

  - Handling Log Consistency: The leader must ensure that the followers' logs
    are consistent with its own. It does this by checking the consistency of
    the logs with each follower. If there's a mismatch, the leader will resend
    missing or conflicting entries to bring the followers' logs into alignment
    with its own.

  - Committing Entries: After successfully replicating a log entry to the
    majority of the followers, the leader marks the entry as committed. This
    means the entry is safely stored and can be applied to the state machines
    of all nodes. The leader also communicates to followers which entries are
    safe to commit.

  - Client Response: Once an entry is committed, the leader returns the result
    of the command execution to the client.

  - Handling Failures: If a follower fails to replicate a log entry (due to a
    crash or network issue), the leader retries log replication until it
    succeeds, ensuring that the follower eventually stores all committed
    entries.

  - Recovering from Disconnects: If the leader gets disconnected from the
    cluster (due to network issues or crashes), a new leader will be elected.
    Upon rejoining, the old leader will revert to a follower and update its log
    according to the new leader's log.

  - Processing Read Requests: The leader can directly handle read-only client
    requests, but it typically first ensures it is still the leader by
    confirming it can communicate with the majority of the cluster.

  - Term Change: If the leader receives a message from another node with a
    higher term, it steps down and reverts to a follower, recognizing the
    authority of the higher term.
  """
  @behaviour Bedrock.Raft.Mode

  alias Bedrock.Raft
  alias Bedrock.Raft.Log
  alias Bedrock.Raft.Mode.Leader.FollowerTracking

  import Bedrock.Raft.Telemetry,
    only: [
      track_consensus_reached: 1,
      track_transaction_added: 2,
      track_append_entries_ack_received: 3,
      track_heartbeat: 1,
      track_append_entries_sent: 5
    ]

  @type t :: %__MODULE__{}
  defstruct ~w[
    nodes
    quorum
    term
    id_sequence
    follower_tracking
    cancel_timer_fn
    log
    interface
  ]a

  @doc """
  Create a new leader. We'll send notices to all the nodes, and schedule the
  timer to tick.
  """
  @spec new(
          Raft.election_term(),
          Raft.quorum(),
          [Raft.service()],
          Log.t(),
          interface :: module()
        ) ::
          t()
  def new(term, quorum, nodes, log, interface) do
    %__MODULE__{
      quorum: quorum,
      nodes: nodes,
      term: term,
      id_sequence: 0,
      follower_tracking:
        FollowerTracking.new(nodes,
          initial_transaction_id: Log.newest_safe_transaction_id(log)
        ),
      log: log,
      interface: interface
    }
    |> send_append_entries_to_followers(nodes)
    |> set_timer()
  end

  @spec next_id(t()) :: {:ok, t(), Raft.transaction_id()}
  def next_id(t) do
    next_id = t.id_sequence + 1
    id = Log.new_id(t.log, t.term, t.id_sequence + 1)
    {:ok, %{t | id_sequence: next_id}, id}
  end

  @impl true
  @spec vote_requested(
          t(),
          Raft.election_term(),
          candidate :: Raft.service(),
          candidate_last_transaction_id :: Raft.transaction_id()
        ) :: {:ok, t()} | :become_follower
  def vote_requested(t, term, _, _) when term > t.term, do: :become_follower
  def vote_requested(t, _, _, _), do: {:ok, t}

  @doc """
  """
  @impl true
  @spec vote_received(t(), Raft.election_term(), follower :: Raft.service()) ::
          :become_follower | {:ok, t()}
  def vote_received(t, term, _) when term > t.term, do: become_follower(t)
  def vote_received(t, _, _), do: {:ok, t}

  @doc """
  Add a transaction to the log. If the transaction is out of order (the
  transaction is not from this term, or is smaller than the newest transaction),
  we'll return an error.
  """
  @impl true
  @spec add_transaction(t(), transaction_payload :: term()) :: {:ok, t(), Raft.transaction_id()}
  def add_transaction(t, transaction_payload) do
    with {:ok, t, new_txn_id} <- next_id(t),
         last_txn_id <- Log.newest_transaction_id(t.log),
         {:ok, log} <-
           Log.append_transactions(t.log, last_txn_id, [
             {new_txn_id, transaction_payload}
           ]) do
      track_transaction_added(t.term, new_txn_id)

      t =
        %{t | log: log}
        |> send_append_entries_to_followers(t.nodes)

      {:ok, t, new_txn_id}
    end
  end

  @doc """
  A follower has responded to our ping. If we haven't recorded them yet for this
  round, do so now.
  """
  @impl true
  @spec append_entries_ack_received(
          t(),
          Raft.election_term(),
          newest_transaction_id :: Raft.transaction_id(),
          follower :: Raft.service()
        ) ::
          {:ok, t()}
  def append_entries_ack_received(t, term, newest_transaction_id, _from = follower)
      when term == t.term do
    track_append_entries_ack_received(term, follower, newest_transaction_id)

    if newest_transaction_id <= Log.newest_transaction_id(t.log) do
      FollowerTracking.update_newest_transaction_id(
        t.follower_tracking,
        follower,
        newest_transaction_id
      )
    end

    t
    |> try_to_reach_consensus(
      FollowerTracking.newest_safe_transaction_id(t.follower_tracking, t.quorum)
    )
    |> then(&{:ok, &1})
  end

  def append_entries_ack_received(t, term, _, _) when term > t.term, do: become_follower(t)
  def append_entries_ack_received(t, _, _, _), do: {:ok, t}

  @doc """
  A ping that is normally directed at a follower has been received. If the term
  is greater than our term, then we will cancel any outstanding timers and
  signal that a new leader has been elected. Otherwise, we'll ignore the ping.
  """
  @impl true
  @spec append_entries_received(
          t(),
          leader_term :: Raft.election_term(),
          prev_transaction_id :: Raft.transaction_id(),
          transactions :: [Raft.transaction()],
          commit_transaction_id :: Raft.transaction_id(),
          from :: Raft.service()
        ) ::
          {:ok, t()} | :become_follower
  def append_entries_received(t, term, _, _, _, _) when term > t.term, do: t |> become_follower()
  def append_entries_received(t, _, _, _, _, _), do: {:ok, t}

  @doc """
  The timer has ticked. We'll send notices to all the nodes, and start the
  timer again.
  """
  @impl true
  @spec timer_ticked(t(), :heartbeat) :: {:ok, t()}
  def timer_ticked(t, :heartbeat) do
    track_heartbeat(t.term)

    t
    |> send_append_entries_to_followers(
      t.follower_tracking
      |> FollowerTracking.followers_not_seen_in(t.interface.heartbeat_ms())
    )
    |> reset_timer()
    |> then(&{:ok, &1})
  end

  def timer_ticked(t, _), do: {:ok, t}

  @spec send_append_entries_to_followers(t(), nodes :: [Raft.service()]) :: t()
  defp send_append_entries_to_followers(t, nodes) do
    newest_safe_transaction_id =
      FollowerTracking.newest_safe_transaction_id(t.follower_tracking, t.quorum)

    nodes
    |> Enum.each(fn follower ->
      prev_transaction_id =
        FollowerTracking.last_sent_transaction_id(t.follower_tracking, follower)

      transactions =
        Log.transactions_from(t.log, prev_transaction_id, :newest)
        |> Enum.take(10)

      if transactions != [] do
        FollowerTracking.update_last_sent_transaction_id(
          t.follower_tracking,
          follower,
          transactions |> List.last() |> elem(0)
        )
      end

      track_append_entries_sent(
        follower,
        t.term,
        prev_transaction_id,
        transactions |> Enum.map(&elem(&1, 0)),
        newest_safe_transaction_id
      )

      apply(t.interface, :send_event, [
        follower,
        {:append_entries, t.term, prev_transaction_id, transactions, newest_safe_transaction_id}
      ])
    end)

    t
  end

  defp try_to_reach_consensus(t, transaction_id) do
    if transaction_id > Log.newest_safe_transaction_id(t.log) do
      track_consensus_reached(transaction_id)

      {:ok, log} = Log.commit_up_to(t.log, transaction_id)
      :ok = apply(t.interface, :consensus_reached, [t.log, transaction_id])

      %{t | log: log}
      |> send_append_entries_to_followers(t.nodes)
    else
      t
    end
  end

  defp become_follower(t) do
    t |> cancel_timer()
    :become_follower
  end

  @spec reset_timer(t()) :: t()
  defp reset_timer(t), do: t |> cancel_timer() |> set_timer()

  @spec cancel_timer(t()) :: t()
  defp cancel_timer(t) when is_nil(t.cancel_timer_fn), do: t

  defp cancel_timer(t) do
    t.cancel_timer_fn.()
    %{t | cancel_timer_fn: nil}
  end

  @spec set_timer(t()) :: t()
  defp set_timer(t), do: %{t | cancel_timer_fn: apply(t.interface, :timer, [:heartbeat])}
end
