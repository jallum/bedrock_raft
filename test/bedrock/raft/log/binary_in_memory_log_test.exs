defmodule Bedrock.Raft.Log.BinaryInMemoryLogTest do
  use ExUnit.Case, async: true
  alias Bedrock.Raft.Log
  alias Bedrock.Raft.Log.BinaryInMemoryLog
  alias Bedrock.Raft.TransactionID

  setup do
    log = BinaryInMemoryLog.new()

    {:ok, log: log}
  end

  describe "new/0" do
    test "creates a new binary in-memory log", %{log: log} do
      assert log.transactions != nil
    end
  end

  describe "new_id/1" do
    test "returns the initial transaction ID", %{log: log} do
      assert Log.new_id(log, 0, 0) == TransactionID.encode({0, 0})
    end

    test "returns a new transaction ID", %{log: log} do
      assert Log.new_id(log, 0, 1) == TransactionID.encode({0, 1})
    end
  end

  describe "append_transactions/3" do
    test "appends transactions to the log when the log is empty", %{log: log} do
      transaction_id_0 = TransactionID.encode({0, 0})

      transaction_id = TransactionID.encode({0, 1})
      transaction = {transaction_id, :some_data}
      {:ok, updated_log} = Log.append_transactions(log, transaction_id_0, [transaction])

      assert :ets.lookup(updated_log.transactions, transaction_id) == [transaction]
    end

    test "appends transactions to the log when the log has entries", %{log: log} do
      transaction_id_0 = TransactionID.encode({0, 0})

      transaction_1_id = TransactionID.encode({0, 1})
      transaction_1 = {transaction_1_id, :some_data}
      {:ok, log} = Log.append_transactions(log, transaction_id_0, [transaction_1])

      transaction_2_id = TransactionID.encode({0, 2})
      transaction_2 = {transaction_2_id, :some_more_data}
      {:ok, log} = Log.append_transactions(log, transaction_1_id, [transaction_2])

      assert Log.transactions_from(log, transaction_1_id, :newest) == [
               transaction_2
             ]

      assert Log.transactions_from(log, transaction_2_id, :newest) == []
    end

    test "returns an error when the previous transaction is not found", %{log: log} do
      transaction_id = TransactionID.encode({0, 1})
      transaction = {transaction_id, :some_data}

      assert {:error, :prev_transaction_not_found} ==
               Log.append_transactions(log, TransactionID.encode({5, 5}), [
                 transaction
               ])
    end
  end

  describe "commit_up_to/2" do
    test "commits transactions up to the given transaction ID", %{log: log} do
      transaction_id = TransactionID.encode({0, 1})
      transaction = {transaction_id, :some_data}

      {:ok, updated_log} =
        Log.append_transactions(log, TransactionID.encode({0, 0}), [transaction])

      {:ok, committed_log} = Log.commit_up_to(updated_log, transaction_id)

      assert committed_log.last_commit == transaction_id
    end
  end

  describe "initial_transaction_id/1" do
    test "returns the initial transaction ID", %{log: log} do
      assert Log.initial_transaction_id(log) == TransactionID.encode({0, 0})
    end
  end

  describe "newest_transaction_id/1" do
    test "returns nil when there are no transactions", %{log: log} do
      assert Log.newest_transaction_id(log) == TransactionID.encode({0, 0})
    end

    test "returns the newest transaction ID", %{log: log} do
      transaction_id = TransactionID.encode({0, 1})
      transaction = {transaction_id, :some_data}

      {:ok, updated_log} =
        Log.append_transactions(log, TransactionID.encode({0, 0}), [transaction])

      assert Log.newest_transaction_id(updated_log) == transaction_id
    end
  end

  describe "newest_safe_transaction_id/1" do
    test "returns the initial transaction ID when there are no transactions", %{log: log} do
      assert Log.newest_safe_transaction_id(log) == TransactionID.encode({0, 0})
    end

    test "returns the last committed transaction ID", %{log: log} do
      transaction_id = TransactionID.encode({0, 1})
      transaction = {transaction_id, :some_data}

      {:ok, updated_log} =
        Log.append_transactions(log, TransactionID.encode({0, 0}), [transaction])

      {:ok, committed_log} = Log.commit_up_to(updated_log, transaction_id)

      assert Log.newest_safe_transaction_id(committed_log) == transaction_id
    end
  end

  describe "has_transaction_id?/2" do
    test "returns true when the transaction ID is the initial transaction ID", %{log: log} do
      assert Log.has_transaction_id?(log, TransactionID.encode({0, 0}))
    end

    test "returns false when the transaction ID is not in the log", %{log: log} do
      assert !Log.has_transaction_id?(log, TransactionID.encode({0, 1}))
    end

    test "returns true when the transaction ID is in the log", %{log: log} do
      transaction_id = TransactionID.encode({0, 1})
      transaction = {transaction_id, :some_data}

      {:ok, updated_log} =
        Log.append_transactions(log, TransactionID.encode({0, 0}), [transaction])

      assert Log.has_transaction_id?(updated_log, transaction_id)
    end
  end

  describe "transactions_to/2" do
    test "returns all transactions up to the newest transaction ID", %{log: log} do
      transaction_id = TransactionID.encode({0, 1})
      transaction = {transaction_id, :some_data}

      {:ok, updated_log} =
        Log.append_transactions(log, TransactionID.encode({0, 0}), [transaction])

      assert Log.transactions_to(updated_log, :newest) == [transaction]
    end

    test "returns all transactions up to the newest safe transaction ID", %{log: log} do
      transaction_id = TransactionID.encode({0, 1})
      transaction = {transaction_id, :some_data}

      {:ok, updated_log} =
        Log.append_transactions(log, TransactionID.encode({0, 0}), [transaction])

      {:ok, committed_log} = Log.commit_up_to(updated_log, transaction_id)

      assert Log.transactions_to(committed_log, :newest_safe) == [transaction]
    end
  end

  describe "transactions_from/3" do
    test "returns all transactions from the given transaction ID to the newest transaction ID", %{
      log: log
    } do
      transaction_id = TransactionID.encode({0, 1})
      transaction = {transaction_id, :some_data}

      {:ok, updated_log} =
        Log.append_transactions(log, TransactionID.encode({0, 0}), [transaction])

      assert Log.transactions_from(updated_log, TransactionID.encode({0, 0}), :newest) == [
               transaction
             ]
    end

    test "returns all transactions from the given transaction ID to the newest safe transaction ID",
         %{log: log} do
      transaction_id = TransactionID.encode({0, 1})
      transaction = {transaction_id, :some_data}

      {:ok, updated_log} =
        Log.append_transactions(log, TransactionID.encode({0, 0}), [transaction])

      {:ok, committed_log} = Log.commit_up_to(updated_log, transaction_id)

      assert Log.transactions_from(committed_log, TransactionID.encode({0, 0}), :newest_safe) == [
               transaction
             ]
    end

    test "returns empty list when from transaction is not found", %{log: log} do
      transaction_id = TransactionID.encode({0, 1})
      transaction = {transaction_id, :some_data}

      {:ok, updated_log} =
        Log.append_transactions(log, TransactionID.encode({0, 0}), [transaction])

      assert Log.transactions_from(updated_log, TransactionID.encode({1, 0}), :newest) == []
    end
  end

  describe "purge_transactions_after/2" do
    test "purges transactions after the given transaction ID", %{log: log} do
      transaction_1_id = TransactionID.encode({0, 1})
      transaction_1 = {transaction_1_id, :data_1}
      transaction_2_id = TransactionID.encode({0, 2})
      transaction_2 = {transaction_2_id, :data_2}

      {:ok, log} = Log.append_transactions(log, TransactionID.encode({0, 0}), [transaction_1])
      {:ok, log} = Log.append_transactions(log, transaction_1_id, [transaction_2])
      {:ok, log} = Log.commit_up_to(log, transaction_2_id)

      {:ok, purged_log} = Log.purge_transactions_after(log, transaction_1_id)

      assert !Log.has_transaction_id?(purged_log, transaction_2_id)
      assert Log.has_transaction_id?(purged_log, transaction_1_id)
      assert purged_log.last_commit == transaction_1_id
    end
  end

  describe "commit_up_to/2 edge cases" do
    test "returns :unchanged when committing to initial transaction ID", %{log: log} do
      assert Log.commit_up_to(log, TransactionID.encode({0, 0})) == :unchanged
    end

    test "returns :unchanged when transaction ID is not newer than last commit", %{log: log} do
      transaction_id = TransactionID.encode({0, 1})
      transaction = {transaction_id, :some_data}

      {:ok, log} = Log.append_transactions(log, TransactionID.encode({0, 0}), [transaction])
      {:ok, committed_log} = Log.commit_up_to(log, transaction_id)

      assert Log.commit_up_to(committed_log, transaction_id) == :unchanged
    end
  end

  describe "append_transactions/3 with tuple transaction IDs" do
    test "accepts transactions with tuple transaction IDs", %{log: log} do
      transaction = {{0, 1}, :some_data}

      {:ok, updated_log} =
        Log.append_transactions(log, TransactionID.encode({0, 0}), [transaction])

      expected_id = TransactionID.encode({0, 1})
      assert Log.has_transaction_id?(updated_log, expected_id)
      assert Log.transactions_to(updated_log, :newest) == [{expected_id, :some_data}]
    end
  end
end
