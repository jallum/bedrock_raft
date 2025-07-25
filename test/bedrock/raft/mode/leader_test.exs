defmodule Bedrock.Raft.Mode.LeaderTest do
  @moduledoc false
  use ExUnit.Case, async: true
  import Mox

  alias Bedrock.Raft.Log.InMemoryLog
  alias Bedrock.Raft.MockInterface
  alias Bedrock.Raft.Mode.Leader

  setup :verify_on_exit!

  def mock_cancel, do: :ok

  setup do
    stub(MockInterface, :quorum_lost, fn _active, _total, _term -> :continue end)
    :ok
  end

  describe "new/5" do
    test "initializes leader with heartbeat timer" do
      term = 2
      quorum = 1
      peers = [:peer_1, :peer_2]
      log = InMemoryLog.new()

      expect(MockInterface, :timestamp_in_ms, fn -> 1000 end)
      expect(MockInterface, :timer, fn :heartbeat -> &mock_cancel/0 end)

      leader = Leader.new(term, quorum, peers, log, MockInterface)

      assert leader.term == term
      assert leader.quorum == quorum
      assert leader.peers == peers
      assert not is_nil(leader.cancel_timer_fn)
    end
  end

  describe "timer_ticked/2" do
    test "continues leadership when heartbeat timer ticks (no forced step-down)" do
      term = 2
      quorum = 1
      peers = [:peer_1, :peer_2]
      log = InMemoryLog.new()

      # Use different timestamps to simulate followers not seen recently
      # initialization
      expect(MockInterface, :timestamp_in_ms, fn -> 1000 end)
      expect(MockInterface, :timer, fn :heartbeat -> &mock_cancel/0 end)
      leader = Leader.new(term, quorum, peers, log, MockInterface)

      # During timer tick, use a later timestamp to make followers appear "not seen recently"
      # Called twice: once in active_followers, once in send_heartbeats_and_continue
      expect(MockInterface, :timestamp_in_ms, 2, fn -> 1200 end)
      # followers not seen in 100ms - called twice (active_followers + send_heartbeats)
      expect(MockInterface, :heartbeat_ms, 2, fn -> 100 end)

      # Now followers should be considered "not seen recently" and get heartbeats
      expect(MockInterface, :send_event, fn :peer_1, {:append_entries, 2, {0, 0}, [], {0, 0}} ->
        :ok
      end)

      expect(MockInterface, :send_event, fn :peer_2, {:append_entries, 2, {0, 0}, [], {0, 0}} ->
        :ok
      end)

      expect(MockInterface, :timer, fn :heartbeat -> &mock_cancel/0 end)

      {:ok, leader} = Leader.timer_ticked(leader, :heartbeat)

      # Leader should still be active (not step down)
      assert leader.term == term
    end

    test "sends heartbeats to followers not recently seen" do
      term = 2
      quorum = 1
      peers = [:peer_1, :peer_2]
      log = InMemoryLog.new()

      # Use different timestamps to simulate passage of time
      # initialization
      expect(MockInterface, :timestamp_in_ms, fn -> 1000 end)
      expect(MockInterface, :timer, fn :heartbeat -> &mock_cancel/0 end)
      leader = Leader.new(term, quorum, peers, log, MockInterface)

      # During timer tick, use a later timestamp
      # Called twice: once in active_followers, once in send_heartbeats_and_continue
      expect(MockInterface, :timestamp_in_ms, 2, fn -> 1200 end)
      # check for followers not seen in 100ms - called twice
      expect(MockInterface, :heartbeat_ms, 2, fn -> 100 end)

      # Followers should be considered "not seen recently" and get heartbeats
      expect(MockInterface, :send_event, fn :peer_1, {:append_entries, 2, {0, 0}, [], {0, 0}} ->
        :ok
      end)

      expect(MockInterface, :send_event, fn :peer_2, {:append_entries, 2, {0, 0}, [], {0, 0}} ->
        :ok
      end)

      expect(MockInterface, :timer, fn :heartbeat -> &mock_cancel/0 end)

      {:ok, _leader} = Leader.timer_ticked(leader, :heartbeat)
    end

    test "delegates quorum loss decision to interface" do
      term = 2
      quorum = 2  # Requires 2 followers to maintain quorum
      peers = [:peer_1, :peer_2]
      log = InMemoryLog.new()

      # Use different timestamps to simulate followers becoming inactive
      expect(MockInterface, :timestamp_in_ms, fn -> 1000 end)  # initialization
      expect(MockInterface, :timer, fn :heartbeat -> &mock_cancel/0 end)
      leader = Leader.new(term, quorum, peers, log, MockInterface)

      # During timer tick, use a later timestamp to simulate long inactive period
      expect(MockInterface, :timestamp_in_ms, fn -> 2000 end)  # 1000ms later
      expect(MockInterface, :heartbeat_ms, fn -> 50 end)  # check for followers not seen in 50ms

      # Configure interface to step down when quorum is lost
      expect(MockInterface, :quorum_lost, fn 0, 2, 2 -> :step_down end)

      # Should step down when interface decides to
      result = Leader.timer_ticked(leader, :heartbeat)
      assert result == :become_follower
    end

    test "continues leadership when interface decides to continue despite quorum loss" do
      term = 2
      quorum = 2  # Requires 2 followers to maintain quorum
      peers = [:peer_1, :peer_2]
      log = InMemoryLog.new()

      # Use different timestamps to simulate followers becoming inactive
      expect(MockInterface, :timestamp_in_ms, fn -> 1000 end)  # initialization
      expect(MockInterface, :timer, fn :heartbeat -> &mock_cancel/0 end)
      leader = Leader.new(term, quorum, peers, log, MockInterface)

      # During timer tick, use a later timestamp
      expect(MockInterface, :timestamp_in_ms, 2, fn -> 2000 end)  # called twice in send_heartbeats_and_continue
      expect(MockInterface, :heartbeat_ms, 2, fn -> 50 end)  # called twice

      # Configure interface to continue despite quorum loss
      expect(MockInterface, :quorum_lost, fn 0, 2, 2 -> :continue end)
      
      # Should send heartbeats and continue
      expect(MockInterface, :send_event, fn :peer_1, {:append_entries, 2, {0, 0}, [], {0, 0}} -> :ok end)
      expect(MockInterface, :send_event, fn :peer_2, {:append_entries, 2, {0, 0}, [], {0, 0}} -> :ok end)
      expect(MockInterface, :timer, fn :heartbeat -> &mock_cancel/0 end)

      {:ok, leader} = Leader.timer_ticked(leader, :heartbeat)
      assert leader.term == term
    end
  end

  describe "vote_requested/4" do
    test "steps down when receiving vote request with higher term" do
      term = 2
      quorum = 1
      peers = [:peer_1, :peer_2]
      log = InMemoryLog.new()

      expect(MockInterface, :timestamp_in_ms, fn -> 1000 end)
      expect(MockInterface, :timer, fn :heartbeat -> &mock_cancel/0 end)
      leader = Leader.new(term, quorum, peers, log, MockInterface)

      # Vote request with higher term should cause step-down
      result = Leader.vote_requested(leader, 3, :peer_1, {3, 1})
      assert result == :become_follower
    end

    test "ignores vote request with lower or equal term" do
      term = 2
      quorum = 1
      peers = [:peer_1, :peer_2]
      log = InMemoryLog.new()

      expect(MockInterface, :timestamp_in_ms, fn -> 1000 end)
      expect(MockInterface, :timer, fn :heartbeat -> &mock_cancel/0 end)
      leader = Leader.new(term, quorum, peers, log, MockInterface)

      # Vote request with same term should be ignored
      {:ok, leader} = Leader.vote_requested(leader, 2, :peer_1, {2, 1})
      assert leader.term == 2

      # Vote request with lower term should be ignored
      {:ok, leader} = Leader.vote_requested(leader, 1, :peer_1, {1, 1})
      assert leader.term == 2
    end
  end

  describe "append_entries_received/6" do
    test "steps down when receiving append_entries with higher term" do
      term = 2
      quorum = 1
      peers = [:peer_1, :peer_2]
      log = InMemoryLog.new()

      expect(MockInterface, :timestamp_in_ms, fn -> 1000 end)
      expect(MockInterface, :timer, fn :heartbeat -> &mock_cancel/0 end)
      leader = Leader.new(term, quorum, peers, log, MockInterface)

      # AppendEntries with higher term should cause step-down
      result = Leader.append_entries_received(leader, 3, {0, 0}, [], {0, 0}, :peer_1)
      assert result == :become_follower
    end

    test "ignores append_entries with lower or equal term" do
      term = 2
      quorum = 1
      peers = [:peer_1, :peer_2]
      log = InMemoryLog.new()

      expect(MockInterface, :timestamp_in_ms, fn -> 1000 end)
      expect(MockInterface, :timer, fn :heartbeat -> &mock_cancel/0 end)
      leader = Leader.new(term, quorum, peers, log, MockInterface)

      # AppendEntries with lower term should be ignored
      {:ok, leader} = Leader.append_entries_received(leader, 1, {0, 0}, [], {0, 0}, :peer_1)
      assert leader.term == 2
    end
  end

  describe "add_transaction/2" do
    test "successfully adds transaction and sends append_entries to followers" do
      term = 2
      quorum = 1
      peers = [:peer_1, :peer_2]
      log = InMemoryLog.new()

      expect(MockInterface, :timestamp_in_ms, fn -> 1000 end)
      expect(MockInterface, :timer, fn :heartbeat -> &mock_cancel/0 end)
      leader = Leader.new(term, quorum, peers, log, MockInterface)

      # Expect append_entries to be sent to both peers after transaction addition
      expect(MockInterface, :send_event, fn :peer_1,
                                            {:append_entries, 2, {0, 0}, [{{2, 1}, "test_data"}],
                                             {0, 0}} ->
        :ok
      end)

      expect(MockInterface, :send_event, fn :peer_2,
                                            {:append_entries, 2, {0, 0}, [{{2, 1}, "test_data"}],
                                             {0, 0}} ->
        :ok
      end)

      {:ok, leader, txn_id} = Leader.add_transaction(leader, "test_data")

      assert txn_id == {2, 1}
      assert leader.id_sequence == 1
    end
  end
end
