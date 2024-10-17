defmodule Bedrock.Raft.Mode.Leader.FollowerTracking do
  @moduledoc """
  The FollowerTracking module is responsible for keeping track of the state of
  followers. This includes the last transaction id that was sent to a follower,
  the newest transaction id that the follower has acknowledged, and the newest
  transaction id that a quorum of followers has acknowledged. This information
  is used to determine the commit index.
  """
  alias Bedrock.Raft

  @type timestamp_fn() :: (-> integer())
  @type t :: %__MODULE__{
          table: :ets.table(),
          timestamp_fn: timestamp_fn()
        }

  defstruct table: nil,
            timestamp_fn: nil

  defp default_timestamp_impl, do: :os.system_time(:millisecond)

  @spec new(
          followers :: [Raft.peer()],
          opts :: [
            initial_transaction_id: Raft.transaction_id(),
            timestamp_fn: timestamp_fn()
          ]
        ) :: t()
  def new(followers, opts) do
    t = %__MODULE__{
      table: :ets.new(:follower_tracking, [:ordered_set]),
      timestamp_fn: opts[:timestamp_fn] || (&default_timestamp_impl/0)
    }

    initial_transaction_id = opts[:initial_transaction_id] || :unknown
    now = timestamp(t)

    :ets.insert(
      t.table,
      followers |> Enum.map(&{&1, initial_transaction_id, initial_transaction_id, now})
    )

    t
  end

  @spec timestamp(t()) :: integer()
  def timestamp(%{timestamp_fn: timestamp_fn}), do: timestamp_fn.()

  @spec last_sent_transaction_id(t(), Raft.peer()) :: Raft.transaction_id()
  def last_sent_transaction_id(t, follower) do
    t.table
    |> :ets.lookup(follower)
    |> case do
      [{^follower, last_sent_transaction_id, _, _}] -> last_sent_transaction_id
      [] -> raise "follower not found: #{inspect(follower)}"
    end
  end

  @spec newest_transaction_id(t(), Raft.peer()) :: Raft.transaction_id()
  def newest_transaction_id(t, follower) do
    t.table
    |> :ets.lookup(follower)
    |> case do
      [{^follower, _, newest_transaction_id, _}] -> newest_transaction_id
      [] -> raise "follower not found: #{inspect(follower)}"
    end
  end

  def followers_not_seen_in(t, n_milliseconds) do
    since = timestamp(t) - n_milliseconds

    t.table
    |> :ets.select([{{:"$1", :_, :_, :"$4"}, [{:>=, since, :"$4"}], [:"$1"]}])
  end

  @doc """
  Find the highest commit that a majority of followers have acknowledged. We
  can do this by sorting the list of last_transaction_id_acked and taking the
  quorum-th-from-last element -- every follower above this index has already
  acknowledged *at-least* up to this transaction.
  """
  @spec newest_safe_transaction_id(t(), quorum :: non_neg_integer()) ::
          Raft.transaction_id()
  def newest_safe_transaction_id(t, quorum) do
    t.table
    |> :ets.select([{{:_, :_, :"$3", :_}, [], [:"$3"]}])
    |> Enum.sort()
    |> Enum.at(-quorum)
  end

  @spec update_last_sent_transaction_id(t(), Raft.peer(), Raft.transaction_id()) :: t()
  def update_last_sent_transaction_id(t, follower, last_transaction_id_sent) do
    t.table |> :ets.update_element(follower, {2, last_transaction_id_sent})
    t
  end

  @spec update_newest_transaction_id(t(), Raft.peer(), Raft.transaction_id()) :: t()
  def update_newest_transaction_id(t, follower, newest_transaction_id) do
    now = timestamp(t)
    t.table |> :ets.insert({follower, newest_transaction_id, newest_transaction_id, now})
    t
  end
end
