defmodule Bedrock.Raft.Mode.FollowerTest do
  use ExUnit.Case, async: true
  import Mox

  alias Bedrock.Raft.Log
  alias Bedrock.Raft.Log.InMemoryLog
  alias Bedrock.Raft.MockInterface
  alias Bedrock.Raft.Mode.Follower

  setup :verify_on_exit!

  def mock_cancel, do: :ok

  describe "new/3" do
    test "initializes with given term and log" do
      term = 1
      log = InMemoryLog.new()

      expect(MockInterface, :timer, fn _, _, _ -> &mock_cancel/0 end)

      follower = Follower.new(term, log, MockInterface)
      assert follower.term == term
      assert follower.leader == :undecided
      assert not is_nil(follower.cancel_timer_fn)
    end
  end

  describe "vote_requested/4" do
    test "votes for a candidate when conditions are met" do
      term = 1
      log = InMemoryLog.new()
      candidate = :node_1
      candidate_last_transaction = :some_tx_id

      expect(MockInterface, :timer, fn _, _, _ -> &mock_cancel/0 end)
      follower = Follower.new(term, log, MockInterface)

      expect(MockInterface, :timer, fn _, _, _ -> &mock_cancel/0 end)
      expect(MockInterface, :send_event, fn ^candidate, {:vote, ^term} -> :ok end)

      {:ok, follower} =
        Follower.vote_requested(follower, term, candidate, candidate_last_transaction)

      assert follower.voted_for == candidate
    end

    test "does not vote for a candidate when already voted for another" do
      term = 1
      log = InMemoryLog.new()
      candidate = :node_1
      candidate_last_transaction = :some_tx_id

      expect(MockInterface, :timer, fn _, _, _ -> &mock_cancel/0 end)
      follower = Follower.new(term, log, MockInterface)

      expect(MockInterface, :timer, fn _, _, _ -> &mock_cancel/0 end)
      expect(MockInterface, :send_event, fn ^candidate, {:vote, ^term} -> :ok end)

      {:ok, follower} =
        Follower.vote_requested(follower, term, candidate, candidate_last_transaction)

      assert follower.voted_for == candidate

      candidate = :node_2
      candidate_last_transaction = :another_tx_id

      {:ok, follower} =
        Follower.vote_requested(follower, term, candidate, candidate_last_transaction)

      assert follower.voted_for == :node_1
    end
  end

  describe "append_entries_received/6" do
    test "cancels timer and elects new leader when leader's term is greater" do
      term = 1
      log = InMemoryLog.new()

      expect(MockInterface, :timer, fn _, _, _ -> &mock_cancel/0 end)
      follower = Follower.new(term, log, MockInterface)

      prev_transaction = {0, 0}
      transactions = [{{2, 1}, "another_tx"}]
      commit_transaction = {2, 1}
      leader = :node_1

      expect(MockInterface, :timer, fn _, _, _ -> &mock_cancel/0 end)

      expect(MockInterface, :consensus_reached, fn log, {2, 1} ->
        assert Log.newest_safe_transaction_id(log) == {2, 1}
        :ok
      end)

      expect(MockInterface, :send_event, fn :node_1, {:append_entries_ack, 2, {2, 1}} -> :ok end)

      {:ok, follower, :new_leader_elected} =
        Follower.append_entries_received(
          follower,
          2,
          prev_transaction,
          transactions,
          commit_transaction,
          leader
        )

      assert follower.term == 2
    end

    test "ignores append entries with a lower term" do
      term = 2
      log = InMemoryLog.new()

      expect(MockInterface, :timer, fn _, _, _ -> &mock_cancel/0 end)
      follower = Follower.new(term, log, MockInterface)

      prev_transaction = :some_tx_id
      transactions = []
      commit_transaction = :another_tx_id
      leader = :node_1

      {:ok, follower} =
        Follower.append_entries_received(
          follower,
          1,
          prev_transaction,
          transactions,
          commit_transaction,
          leader
        )

      assert follower.leader == :undecided
    end
  end
end