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

  test "newly initialized followers should not be considered not seen", %{t: t} do
    # Immediately after initialization (same timestamp), no followers should be "not seen"
    expect(MockTimer, :timestamp, fn -> 1000 end)
    assert [] = FollowerTracking.followers_not_seen_in(t, 0) |> Enum.sort()

    # A few milliseconds later, if we check within the timeframe, no followers should be "not seen"
    expect(MockTimer, :timestamp, fn -> 1025 end)
    assert [] = FollowerTracking.followers_not_seen_in(t, 30) |> Enum.sort()

    # But if we check with a shorter timeframe, they should be "not seen"
    expect(MockTimer, :timestamp, fn -> 1025 end)
    assert [:f1, :f2, :f3] = FollowerTracking.followers_not_seen_in(t, 20) |> Enum.sort()

    # After heartbeat_ms * 5 (typically 250ms), followers should be "not seen"
    expect(MockTimer, :timestamp, fn -> 1300 end)
    assert [:f1, :f2, :f3] = FollowerTracking.followers_not_seen_in(t, 250) |> Enum.sort()
  end

  test "newest_safe_transaction_id/2 handles edge cases safely", %{t: t} do
    # Test with quorum = 0 (single peer cluster)
    expect(MockTimer, :timestamp, fn -> 2000 end)
    FollowerTracking.update_newest_transaction_id(t, :f1, 5)
    assert FollowerTracking.newest_safe_transaction_id(t, 0) == 5

    # Test with quorum larger than follower count
    # Should return the lowest transaction (most conservative)
    expect(MockTimer, :timestamp, fn -> 2010 end)
    FollowerTracking.update_newest_transaction_id(t, :f2, 3)
    expect(MockTimer, :timestamp, fn -> 2020 end)
    FollowerTracking.update_newest_transaction_id(t, :f3, 7)
    assert FollowerTracking.newest_safe_transaction_id(t, 10) == 3

    # Test with empty follower set (edge case)
    expect(MockTimer, :timestamp, fn -> 2030 end)

    empty_t =
      FollowerTracking.new([], initial_transaction_id: 1, timestamp_fn: &MockTimer.timestamp/0)

    assert FollowerTracking.newest_safe_transaction_id(empty_t, 1) == nil
  end
end
