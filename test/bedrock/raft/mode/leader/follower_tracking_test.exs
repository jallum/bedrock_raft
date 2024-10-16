defmodule Bedrock.Raft.Mode.Leader.FollowerTrackingTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Bedrock.Raft.Mode.Leader.FollowerTracking

  @followers [:f1, :f2, :f3]

  setup do
    {:ok, table: FollowerTracking.new(@followers, :unknown)}
  end

  test "new/1 initializes the ETS table with followers", %{table: table} do
    assert :ets.lookup(table, :f1) == [{:f1, :unknown, :unknown}]
    assert :ets.lookup(table, :f2) == [{:f2, :unknown, :unknown}]
    assert :ets.lookup(table, :f3) == [{:f3, :unknown, :unknown}]
  end

  test "last_sent_transaction_id/2 returns the last sent transaction id", %{table: table} do
    assert FollowerTracking.last_sent_transaction_id(table, :f1) == :unknown
    FollowerTracking.update_last_sent_transaction_id(table, :f1, 1)
    assert FollowerTracking.last_sent_transaction_id(table, :f1) == 1
  end

  test "newest_transaction_id/2 returns the newest transaction id", %{table: table} do
    assert FollowerTracking.newest_transaction_id(table, :f1) == :unknown
    FollowerTracking.update_newest_transaction_id(table, :f1, 2)
    assert FollowerTracking.newest_transaction_id(table, :f1) == 2
  end

  test "newest_safe_transaction_id/2 returns the highest commit acknowledged by a quorum", %{
    table: table
  } do
    FollowerTracking.update_newest_transaction_id(table, :f1, 1)
    FollowerTracking.update_newest_transaction_id(table, :f2, 2)
    FollowerTracking.update_newest_transaction_id(table, :f3, 3)
    assert FollowerTracking.newest_safe_transaction_id(table, 1) == 3
    assert FollowerTracking.newest_safe_transaction_id(table, 2) == 2
    assert FollowerTracking.newest_safe_transaction_id(table, 3) == 1
  end

  test "update_last_sent_transaction_id/3 updates the last sent transaction id", %{table: table} do
    FollowerTracking.update_last_sent_transaction_id(table, :f1, 1)
    assert :ets.lookup(table, :f1) == [{:f1, 1, :unknown}]
  end

  test "update_newest_transaction_id/3 updates the newest transaction id and the last sent transaction id",
       %{table: table} do
    FollowerTracking.update_newest_transaction_id(table, :f1, 2)
    assert :ets.lookup(table, :f1) == [{:f1, 2, 2}]
  end
end
