defmodule Bedrock.Raft.Mode.FollowerTest do
  @moduledoc false
  use ExUnit.Case, async: true
  import Mox

  alias Bedrock.Raft.Log.InMemoryLog
  alias Bedrock.Raft.MockInterface
  alias Bedrock.Raft.Mode.Follower

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
      expect(MockInterface, :consensus_reached, fn _, ^t1 -> :ok end)

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
  end
end
