defmodule Bedrock.Raft.Mode.Leader.FollowerTrackingTest do
  @moduledoc false
  use ExUnit.Case, async: true

  import Mox

  alias Bedrock.Raft.Mode.Leader.FollowerTracking

  @followers [:f1, :f2, :f3]

  defmodule Timer do
    @callback timestamp() :: integer()
  end

  defmock(MockTimer, for: Timer)

  setup do
    expect(MockTimer, :timestamp, fn -> 1000 end)

    t =
      FollowerTracking.new(@followers,
        initial_transaction_id: :unknown,
        timestamp_fn: &MockTimer.timestamp/0
      )

    {:ok, t: t}
  end

  setup :verify_on_exit!

  test "new/1 initializes the ETS table with followers", %{t: t} do
    assert [{:f1, :unknown, :unknown, 1000}] = :ets.lookup(t.table, :f1)
    assert [{:f2, :unknown, :unknown, 1000}] = :ets.lookup(t.table, :f2)
    assert [{:f3, :unknown, :unknown, 1000}] = :ets.lookup(t.table, :f3)
  end

  test "last_sent_transaction_id/2 returns the last sent transaction id", %{t: t} do
    assert FollowerTracking.last_sent_transaction_id(t, :f1) == :unknown
    FollowerTracking.update_last_sent_transaction_id(t, :f1, 1)
    assert FollowerTracking.last_sent_transaction_id(t, :f1) == 1
  end

  test "newest_transaction_id/2 returns the newest transaction id", %{t: t} do
    expect(MockTimer, :timestamp, fn -> 1020 end)

    assert FollowerTracking.newest_transaction_id(t, :f1) == :unknown
    FollowerTracking.update_newest_transaction_id(t, :f1, 2)
    assert FollowerTracking.newest_transaction_id(t, :f1) == 2
  end

  test "newest_safe_transaction_id/2 returns the highest commit acknowledged by a quorum", %{
    t: t
  } do
    expect(MockTimer, :timestamp, fn -> 1020 end)
    FollowerTracking.update_newest_transaction_id(t, :f1, 1)

    expect(MockTimer, :timestamp, fn -> 1040 end)
    FollowerTracking.update_newest_transaction_id(t, :f2, 2)

    expect(MockTimer, :timestamp, fn -> 1060 end)
    FollowerTracking.update_newest_transaction_id(t, :f3, 3)

    assert FollowerTracking.newest_safe_transaction_id(t, 1) == 3
    assert FollowerTracking.newest_safe_transaction_id(t, 2) == 2
    assert FollowerTracking.newest_safe_transaction_id(t, 3) == 1
  end

  test "update_last_sent_transaction_id/3 updates the last sent transaction id", %{t: t} do
    FollowerTracking.update_last_sent_transaction_id(t, :f1, 1)
    assert [{:f1, 1, :unknown, _}] = :ets.lookup(t.table, :f1)
  end

  test "update_newest_transaction_id/3 updates the newest transaction id, the last sent transaction id and the timestamp",
       %{t: t} do
    expect(MockTimer, :timestamp, fn -> 1080 end)
    FollowerTracking.update_newest_transaction_id(t, :f1, 2)
    assert [{:f1, 2, 2, 1080}] = :ets.lookup(t.table, :f1)
  end

  test "followers_not_seen_in/2 returns the followers that have not been seen in the last n milliseconds",
       %{t: t} do
    expect(MockTimer, :timestamp, fn -> 1100 end)
    FollowerTracking.update_newest_transaction_id(t, :f1, 1)

    expect(MockTimer, :timestamp, fn -> 1120 end)
    FollowerTracking.update_newest_transaction_id(t, :f2, 2)

    expect(MockTimer, :timestamp, fn -> 1140 end)
    FollowerTracking.update_newest_transaction_id(t, :f3, 3)

    expect(MockTimer, :timestamp, fn -> 1150 end)
    assert [:f1, :f2, :f3] = FollowerTracking.followers_not_seen_in(t, 5) |> Enum.sort()

    expect(MockTimer, :timestamp, fn -> 1150 end)
    assert [:f1, :f2] = FollowerTracking.followers_not_seen_in(t, 15) |> Enum.sort()

    expect(MockTimer, :timestamp, fn -> 1150 end)
    assert [:f1] = FollowerTracking.followers_not_seen_in(t, 35) |> Enum.sort()

    expect(MockTimer, :timestamp, fn -> 1150 end)
    assert [] = FollowerTracking.followers_not_seen_in(t, 55) |> Enum.sort()
  end
end
