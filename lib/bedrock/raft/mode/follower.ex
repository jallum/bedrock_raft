defmodule Bedrock.Raft.Mode.Follower do
  @moduledoc """
  The Follower state of the RAFT protocol. This is the state that a node is in
  when it is not the leader, and is waiting for a leader to emerge, but has not
  yet decided to stand as a candidate for election itself.

  These are the key characteristics of this phase:

  - Passivity: Followers are mostly passive. They don't issue requests on
    their own but respond to requests from leaders and candidates.

  - Responding to Leaders: When a follower receives a heartbeat (a signal to
    prevent election timeouts) or data from the current leader, it updates its
    internal state and then responds to the leader’s requests.

  - Election Timeout: Each follower has an election timeout. If a follower
    doesn't hear from a leader before this timeout, it assumes there is no
    current leader and indicates that a transition to the Candidate state is
    necessary to start an election.

  - Log Replication: Followers receive log entries from the leader. They append
    these entries to their log and return acknowledgment to the leader.

  - Committing Entries: Once a log entry is safely replicated, the leader
    informs the followers to commit the entry. The followers then apply the
    entry to their local state machines.

  - Consistency Check: If there's a discrepancy in the log entries between a
    leader and a follower, the follower rejects the new entries. The leader
    then works to bring the follower's log into consistency with its own.
  """
  @behaviour Bedrock.Raft.Mode

  @type t :: %__MODULE__{}
  defstruct ~w[
      term
      leader
      voted_for
      last_consensus_transaction_id
      cancel_timer_fn
      log
      interface
    ]a

  alias Bedrock.Raft
  alias Bedrock.Raft.Log

  @spec new(Raft.election_term(), Log.t(), interface :: module(), leader :: Raft.service()) :: t()
  def new(term, log, interface, leader \\ :undecided)

  def new(term, log, interface, :undecided) do
    %__MODULE__{
      term: term,
      voted_for: nil,
      last_consensus_transaction_id: Log.newest_safe_transaction_id(log),
      leader: :undecided,
      log: log,
      interface: interface
    }
    |> set_timer()
  end

  def new(term, log, interface, leader) do
    %__MODULE__{
      term: term,
      voted_for: leader,
      last_consensus_transaction_id: Log.newest_safe_transaction_id(log),
      leader: leader,
      log: log,
      interface: interface
    }
    |> set_timer()
    |> send_append_entries_ack()
  end

  @doc """
  Our vote has been requested by a candidate. We'll vote for the candidate if
  we haven't yet voted in this election, or:
    - the candidate's election term is greater or equal to our term, and
    - the candidate's last transaction is at least as up-to-date as ours.

  Otherwise, we'll ignore the request.
  """
  @impl true
  @spec vote_requested(
          t(),
          Raft.election_term(),
          candidate :: Raft.service(),
          candidate_last_transaction :: Raft.transaction()
        ) :: {:ok, t()}
  def vote_requested(t, term, candidate, candidate_newest_transaction_id) do
    if (term == t.term and is_nil(t.voted_for)) or
         (term > t.term and candidate_newest_transaction_id >= Log.newest_transaction_id(t.log)) do
      {:ok,
       t
       |> reset_timer()
       |> vote_for(candidate, term)}
    else
      {:ok, t}
    end
  end

  @doc """
  """
  @impl true
  @spec vote_received(t(), Raft.election_term(), follower :: Raft.service()) ::
          :become_follower | {:ok, t()}
  def vote_received(t, term, _) when term > t.term, do: become_follower(t)
  def vote_received(t, _, _), do: {:ok, t}

  @doc """
  As a follower, we cannot add transactions. So we don't.
  """
  @impl true
  @spec add_transaction(t(), transaction_payload :: term()) :: {:error, :not_leader}
  def add_transaction(_t, _transaction_payload), do: {:error, :not_leader}

  @doc """
  As a follower, we don't need to do anything when we receive an append entries
  acknowledgement. If the term is greater than our term, then we will cancel
  any outstanding timers and record that a new leader has been elected.
  Otherwise, we'll ignore it.
  """
  @impl true
  @spec append_entries_ack_received(
          t(),
          Raft.election_term(),
          newest_transaction_id :: Raft.transaction_id(),
          follower :: Raft.service()
        ) :: {:ok, t()} | :become_follower
  def append_entries_ack_received(t, term, _, _) when term > t.term, do: t |> become_follower()
  def append_entries_ack_received(t, _, _, _), do: {:ok, t}

  @doc """
  A ping has been received. If the term is greater than our term, then we will
  cancel any outstanding timers and record that a new leader has been elected.
  Otherwise, we'll ignore it.
  """
  @impl true
  @spec append_entries_received(
          t(),
          term :: Raft.election_term(),
          prev_transaction_id :: Raft.transaction_id(),
          transactions :: [Raft.transaction()],
          commit_transaction_id :: Raft.transaction_id(),
          from :: Raft.service()
        ) ::
          {:ok, t()} | :become_follower
  def append_entries_received(
        t,
        term,
        prev_transaction_id,
        transactions,
        commit_transaction_id,
        from
      )
      when term == t.term and t.leader in [:undecided, from] do
    t
    |> reset_timer()
    |> note_change_in_leadership_if_necessary(from)
    |> try_to_append_transactions(prev_transaction_id, transactions)
    |> try_commit_up_to(commit_transaction_id)
    |> send_append_entries_ack()
    |> then(&{:ok, &1})
  end

  def append_entries_received(t, term, _, _, _, _) when term > t.term, do: t |> become_follower()
  def append_entries_received(t, _, _, _, _, _), do: {:ok, t}

  @spec timer_ticked(t()) :: :become_candidate
  def timer_ticked(t), do: t |> become_candidate()

  def try_to_append_transactions(t, _prev_transaction, []), do: t

  def try_to_append_transactions(t, prev_transaction_id, transactions) do
    {prev_transaction_id, transactions} =
      skip_transactions_already_in_log(
        prev_transaction_id,
        transactions,
        Log.newest_transaction_id(t.log)
      )

    Log.append_transactions(t.log, prev_transaction_id, transactions)
    |> case do
      {:ok, log} ->
        %{t | log: log}

      {:error, :prev_transaction_not_found} ->
        t
    end
  end

  @spec skip_transactions_already_in_log(
          prev_txn_id :: Raft.transaction_id(),
          transactions :: [Raft.transaction()],
          newest_txn_id :: Raft.transaction_id()
        ) :: {Raft.transaction_id(), [Raft.transaction()]}
  defp skip_transactions_already_in_log(prev_txn_id, [], _newest_txn_id), do: {prev_txn_id, []}

  defp skip_transactions_already_in_log(
         prev_txn_id,
         [{txn_id, _payload} | remaining_transactions],
         newest_txn_id
       )
       when prev_txn_id < newest_txn_id,
       do: skip_transactions_already_in_log(txn_id, remaining_transactions, newest_txn_id)

  defp skip_transactions_already_in_log(prev_txn_id, transactions, _newest_txn_id),
    do: {prev_txn_id, transactions}

  def try_commit_up_to(t, transaction_id) do
    consensus_transaction_id = min(Log.newest_transaction_id(t.log), transaction_id)

    {:ok, log} = Log.commit_up_to(t.log, consensus_transaction_id)

    %{t | log: log}
    |> notify_if_consensus_reached(consensus_transaction_id)
  end

  defp note_change_in_leadership_if_necessary(t, leader) when t.leader == :undecided do
    apply(t.interface, :leadership_changed, [{leader, t.term}])
    %{t | leader: leader}
  end

  defp note_change_in_leadership_if_necessary(t, _), do: t

  defp become_follower(t) do
    t |> cancel_timer()
    :become_follower
  end

  defp become_candidate(t) do
    t |> cancel_timer()
    :become_candidate
  end

  @spec send_append_entries_ack(t()) :: t()
  defp send_append_entries_ack(t) do
    apply(t.interface, :send_event, [
      t.leader,
      {:append_entries_ack, t.term, Log.newest_transaction_id(t.log)}
    ])

    t
  end

  @spec vote_for(t(), Raft.service(), Raft.election_term()) :: t()
  defp vote_for(t, candidate, election_term) do
    apply(t.interface, :send_event, [candidate, {:vote, election_term}])
    %{t | voted_for: candidate, term: election_term}
  end

  @spec reset_timer(t()) :: t()
  defp reset_timer(t),
    do: t |> cancel_timer() |> set_timer()

  @spec cancel_timer(t()) :: t()
  def cancel_timer(t) when is_nil(t.cancel_timer_fn), do: t

  def cancel_timer(t) do
    t.cancel_timer_fn.()
    %{t | cancel_timer_fn: nil}
  end

  @spec set_timer(t()) :: t()
  defp set_timer(t),
    do: %{t | cancel_timer_fn: apply(t.interface, :timer, [:election, 150, 300])}

  defp notify_if_consensus_reached(t, transaction_id)
       when t.last_consensus_transaction_id < transaction_id do
    :ok = apply(t.interface, :consensus_reached, [t.log, transaction_id])
    %{t | last_consensus_transaction_id: transaction_id}
  end

  defp notify_if_consensus_reached(t, _transaction_id), do: t
end
