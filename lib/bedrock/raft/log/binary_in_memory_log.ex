defmodule Bedrock.Raft.Log.BinaryInMemoryLog do
  alias Bedrock.Raft
  alias Bedrock.Raft.TransactionID

  @type t :: %__MODULE__{
          transactions: :ets.table(),
          last_commit: Raft.binary_transaction_id() | nil
        }
  defstruct ~w[
    transactions
    last_commit
  ]a

  @spec new() :: t()
  def new,
    do: %__MODULE__{
      transactions: :ets.new(:binary_in_memory_log, [:ordered_set])
    }

  defimpl Bedrock.Raft.Log do
    @type t :: Bedrock.Raft.Log.BinaryInMemoryLog.t()

    @initial_transaction_id TransactionID.encode({0, 0})

    @impl true
    def new_id(_t, term, sequence), do: TransactionID.encode({term, sequence})

    @impl true
    def append_transactions(t, @initial_transaction_id, transactions) do
      true =
        :ets.insert_new(
          t.transactions,
          transactions |> Enum.map(&normalize_transaction(t, &1))
        )

      {:ok, t}
    end

    def append_transactions(t, prev_transaction_id, transactions)
        when is_binary(prev_transaction_id) do
      :ets.lookup(t.transactions, prev_transaction_id)
      |> case do
        [{^prev_transaction_id, _}] ->
          true =
            :ets.insert_new(
              t.transactions,
              transactions |> Enum.map(&normalize_transaction(t, &1))
            )

          {:ok, t}

        [] ->
          {:error, :prev_transaction_not_found}
      end
    end

    @impl true
    def initial_transaction_id(_t), do: @initial_transaction_id

    @impl true
    @spec commit_up_to(t(), Raft.binary_transaction_id()) :: {:ok, t()}
    def commit_up_to(t, transaction_id) when is_binary(transaction_id),
      do: {:ok, %{t | last_commit: transaction_id}}

    @impl true
    def newest_transaction_id(t) do
      :ets.last(t.transactions)
      |> case do
        :"$end_of_table" -> @initial_transaction_id
        transaction_id -> transaction_id
      end
    end

    @impl true
    def newest_safe_transaction_id(t), do: t.last_commit || initial_transaction_id(t)

    @impl true
    def has_transaction_id?(_t, @initial_transaction_id), do: true
    def has_transaction_id?(t, transaction_id), do: :ets.member(t.transactions, transaction_id)

    @impl true
    def transactions_to(t, :newest),
      do: transactions_from(t, initial_transaction_id(t), newest_transaction_id(t))

    def transactions_to(t, :newest_safe),
      do: transactions_from(t, initial_transaction_id(t), newest_safe_transaction_id(t))

    @impl true
    def transactions_from(t, from, :newest),
      do: transactions_from(t, from, newest_transaction_id(t))

    def transactions_from(t, from, :newest_safe),
      do: transactions_from(t, from, newest_safe_transaction_id(t))

    def transactions_from(t, @initial_transaction_id, to),
      do: :ets.select(t.transactions, match_lte(to))

    def transactions_from(t, from, to) do
      :ets.select(t.transactions, match_gte_lte(from, to))
      |> case do
        [{^from, _data} | transactions] -> transactions
        [] -> []
      end
    end

    def match_lte(lte) do
      [
        {{:"$1", :"$2"}, [{:"=<", :"$1", {:const, lte}}], [{{:"$1", :"$2"}}]}
      ]
    end

    def match_gte_lte(gte, lte) do
      [
        {{:"$1", :"$2"}, [{:>=, :"$1", {:const, gte}}, {:"=<", :"$1", {:const, lte}}],
         [{{:"$1", :"$2"}}]}
      ]
    end

    @doc """
    Ensure that the given transaction is in the correct format for the log,
    converting it only if necessary.
    """
    @spec normalize_transaction(t(), Raft.transaction()) :: Raft.transaction()
    def normalize_transaction(_t, {transaction_id, _data} = transaction)
        when is_binary(transaction_id),
        do: transaction

    def normalize_transaction(_t, {transaction_id, data}) when is_tuple(transaction_id),
      do: {transaction_id |> TransactionID.encode(), data}
  end
end
