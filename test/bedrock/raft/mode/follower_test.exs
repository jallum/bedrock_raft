defmodule Bedrock.Raft.Mode.FollowerTest do
  @moduledoc false
  use ExUnit.Case, async: true
  import Mox

  alias Bedrock.Raft.Log.InMemoryLog
  alias Bedrock.Raft.MockInterface
  alias Bedrock.Raft.Mode.Follower
  alias Bedrock.Raft.Log

  setup :verify_on_exit!

  def mock_cancel, do: :ok

  describe "new/3" do
    test "initializes with given term and log" do
      term = 1
      log = InMemoryLog.new()

      expect(MockInterface, :timer, fn _ -> &mock_cancel/0 end)

      follower = Follower.new(term, log, MockInterface, :peer_0)
      assert follower.term == term
      assert follower.leader == :undecided
      assert not is_nil(follower.cancel_timer_fn)
    end
  end

  describe "vote_requested/4" do
    test "votes for a candidate when conditions are met" do
      term = 1
      log = InMemoryLog.new()

      candidate = :peer_1
      candidate_last_transaction = {1, 1}

      expect(MockInterface, :timer, fn _ -> &mock_cancel/0 end)
      follower = Follower.new(term, log, MockInterface, :peer_0)

      expect(MockInterface, :timer, fn _ -> &mock_cancel/0 end)
      expect(MockInterface, :send_event, fn ^candidate, {:vote, ^term} -> :ok end)

      {:ok, follower} =
        Follower.vote_requested(follower, term, candidate, candidate_last_transaction)

      assert follower.voted_for == candidate
    end

    test "rejects candidate with older log term" do
      term = 2
      {:ok, log} = InMemoryLog.new() |> Log.append_transactions({0, 0}, [{{1, 1}, "data1"}])
      {:ok, log} = log |> Log.append_transactions({1, 1}, [{{2, 2}, "data2"}])

      candidate = :peer_1
      candidate_last_transaction = {1, 5}  # older term despite higher index

      expect(MockInterface, :timer, fn _ -> &mock_cancel/0 end)
      follower = Follower.new(term, log, MockInterface, :peer_0)

      {:ok, follower} =
        Follower.vote_requested(follower, term, candidate, candidate_last_transaction)

      assert follower.voted_for == nil
    end

    test "accepts candidate with same term but higher index" do
      term = 2
      {:ok, log} = InMemoryLog.new() |> Log.append_transactions({0, 0}, [{{2, 1}, "data1"}])

      candidate = :peer_1
      candidate_last_transaction = {2, 3}  # same term, higher index

      expect(MockInterface, :timer, fn _ -> &mock_cancel/0 end)
      follower = Follower.new(term, log, MockInterface, :peer_0)

      expect(MockInterface, :timer, fn _ -> &mock_cancel/0 end)
      expect(MockInterface, :send_event, fn ^candidate, {:vote, ^term} -> :ok end)

      {:ok, follower} =
        Follower.vote_requested(follower, term, candidate, candidate_last_transaction)

      assert follower.voted_for == candidate
    end

    test "rejects candidate with same term but lower index" do
      term = 2
      {:ok, log} = InMemoryLog.new() |> Log.append_transactions({0, 0}, [{{2, 5}, "data1"}])

      candidate = :peer_1
      candidate_last_transaction = {2, 3}  # same term, lower index

      expect(MockInterface, :timer, fn _ -> &mock_cancel/0 end)
      follower = Follower.new(term, log, MockInterface, :peer_0)

      {:ok, follower} =
        Follower.vote_requested(follower, term, candidate, candidate_last_transaction)

      assert follower.voted_for == nil
    end

    test "does not vote for a candidate when already voted for another" do
      term = 1
      log = InMemoryLog.new()
      candidate = :peer_1
      candidate_last_transaction = {1, 1}

      expect(MockInterface, :timer, fn _ -> &mock_cancel/0 end)
      follower = Follower.new(term, log, MockInterface, :peer_0)

      expect(MockInterface, :timer, fn _ -> &mock_cancel/0 end)
      expect(MockInterface, :send_event, fn ^candidate, {:vote, ^term} -> :ok end)

      {:ok, follower} =
        Follower.vote_requested(follower, term, candidate, candidate_last_transaction)

      assert follower.voted_for == candidate

      candidate = :peer_2
      candidate_last_transaction = {1, 2}

      {:ok, follower} =
        Follower.vote_requested(follower, term, candidate, candidate_last_transaction)

      assert follower.voted_for == :peer_1
    end
  end

  describe "append_entries_received/6" do
    test "resets timer and acks the new leader when leader's term is greater" do
      term = 1
      log = InMemoryLog.new()

      expect(MockInterface, :timer, fn _ -> &mock_cancel/0 end)
      p = Follower.new(term, log, MockInterface, :peer_0)

      t0 = {0, 0}
      transactions = [{{2, 1}, "another_tx"}]
      t1 = {2, 1}
      leader = :peer_1

      expect(MockInterface, :timer, fn _ -> &mock_cancel/0 end)
      expect(MockInterface, :leadership_changed, fn {^leader, 2} -> :ok end)
      expect(MockInterface, :send_event, fn :peer_1, {:append_entries_ack, 2, ^t1} -> :ok end)
      expect(MockInterface, :consensus_reached, fn _, ^t1, :latest -> :ok end)

      {:ok, p} = Follower.append_entries_received(p, 2, t0, transactions, t1, leader)

      assert ^leader = p.leader
      assert 2 = p.term
    end

    test "ignores append entries with a lower term" do
      term = 2
      log = InMemoryLog.new()

      expect(MockInterface, :timer, fn _ -> &mock_cancel/0 end)
      follower = Follower.new(term, log, MockInterface, :peer_0)

      prev_transaction_id = :some_tx_id
      transactions = []
      commit_transaction_id = :another_tx_id
      leader = :peer_1

      {:ok, follower} =
        Follower.append_entries_received(
          follower,
          1,
          prev_transaction_id,
          transactions,
          commit_transaction_id,
          leader
        )

      assert follower.leader == :undecided
    end

    test "accepts append_entries from any peer with higher term (prevents split-brain)" do
      term = 1
      log = InMemoryLog.new()

      expect(MockInterface, :timer, fn _ -> &mock_cancel/0 end)
      follower = Follower.new(term, log, MockInterface, :peer_0, :peer_1)

      # New leader with higher term should be accepted
      new_leader = :peer_2
      new_term = 2
      t0 = {0, 0}
      transactions = [{{2, 1}, "new_leader_tx"}]
      t1 = {2, 1}

      expect(MockInterface, :timer, fn _ -> &mock_cancel/0 end)
      expect(MockInterface, :leadership_changed, fn {^new_leader, ^new_term} -> :ok end)

      expect(MockInterface, :send_event, fn ^new_leader, {:append_entries_ack, ^new_term, ^t1} ->
        :ok
      end)

      expect(MockInterface, :consensus_reached, fn _, ^t1, :latest -> :ok end)

      {:ok, follower} =
        Follower.append_entries_received(follower, new_term, t0, transactions, t1, new_leader)

      assert follower.leader == new_leader
      assert follower.term == new_term
    end

    test "handles log consistency failure gracefully" do
      term = 2
      {:ok, log} = InMemoryLog.new() |> Log.append_transactions({0, 0}, [{{1, 1}, "existing"}])

      expect(MockInterface, :timer, fn _ -> &mock_cancel/0 end)
      follower = Follower.new(term, log, MockInterface, :peer_0)

      leader = :peer_1
      prev_transaction_id = {1, 99}  # doesn't exist
      transactions = [{{2, 2}, "new_data"}]
      commit_transaction_id = {2, 2}

      expect(MockInterface, :timer, fn _ -> &mock_cancel/0 end)
      expect(MockInterface, :leadership_changed, fn {^leader, ^term} -> :ok end)

      expect(MockInterface, :send_event, fn ^leader, {:append_entries_ack, ^term, {1, 1}} ->
        :ok
      end)

      {:ok, follower} =
        Follower.append_entries_received(
          follower,
          term,
          prev_transaction_id,
          transactions,
          commit_transaction_id,
          leader
        )

      assert follower.leader == leader
      assert follower.term == term
    end
  end
end
