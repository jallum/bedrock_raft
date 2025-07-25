defmodule Bedrock.Raft.Mode.CandidateTest do
  @moduledoc false
  use ExUnit.Case, async: true
  alias Bedrock.Raft.Mode.Candidate
  alias Bedrock.Raft.Log.InMemoryLog
  alias Bedrock.Raft.Log
  import Mox
  setup :verify_on_exit!

  alias Bedrock.Raft.MockInterface

  def mock_cancel, do: :ok

  setup do
    log = InMemoryLog.new()
    {:ok, log: log}
  end

  describe "new/5" do
    test "creates a new candidate and requests votes", %{log: log} do
      peers = [:b, :c]
      expect(MockInterface, :timer, fn :election -> fn -> :ok end end)
      expect(MockInterface, :send_event, 2, fn _, {:request_vote, 1, {0, 0}} -> :ok end)

      candidate = Candidate.new(1, 1, peers, log, MockInterface)

      assert %Candidate{
               term: 1,
               quorum: 1,
               peers: ^peers,
               votes: [],
               log: ^log,
               interface: MockInterface
             } = candidate
    end
  end

  describe "vote_received/3" do
    setup %{log: log} do
      expect(MockInterface, :send_event, fn :b, {:request_vote, 1, {0, 0}} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:request_vote, 1, {0, 0}} -> :ok end)
      expect(MockInterface, :timer, fn :election -> fn -> :ok end end)
      candidate = Candidate.new(1, 2, [:b, :c], log, MockInterface)
      {:ok, candidate: candidate}
    end

    test "records vote and returns :ok when term matches", %{candidate: candidate} do
      {:ok, updated_candidate} = Candidate.vote_received(candidate, 1, :b)
      assert updated_candidate.votes == [:b]
    end

    test "becomes a follower when the voting term is higher", %{candidate: candidate} do
      :become_follower = Candidate.vote_received(candidate, 2, :b)
    end

    test "returns :become_leader when quorum is reached", %{candidate: candidate} do
      {:ok, candidate_with_vote} = Candidate.vote_received(candidate, 1, :b)
      assert :become_leader = Candidate.vote_received(candidate_with_vote, 1, :c)
    end
  end

  describe "vote_requested/4" do
    setup %{log: log} do
      expect(MockInterface, :send_event, 2, fn _, {:request_vote, 1, {0, 0}} -> :ok end)
      expect(MockInterface, :timer, fn :election -> &mock_cancel/0 end)
      candidate = Candidate.new(1, 1, [:b, :c], log, MockInterface)
      {:ok, candidate: candidate}
    end

    test "votes for candidate with more up-to-date log", %{candidate: candidate} do
      candidate_txn_id = {2, 1}

      expect(MockInterface, :send_event, fn :peer_1, {:vote, 1} -> :ok end)

      {:ok, updated_candidate} = Candidate.vote_requested(candidate, 1, :peer_1, candidate_txn_id)
      assert updated_candidate.voted_for == :peer_1
    end

    test "rejects candidate with older log term", %{candidate: _candidate} do
      {:ok, log_with_data} =
        InMemoryLog.new() |> Log.append_transactions({0, 0}, [{{2, 1}, "data"}])

      expect(MockInterface, :send_event, 2, fn _, {:request_vote, 1, {2, 1}} -> :ok end)
      expect(MockInterface, :timer, fn :election -> &mock_cancel/0 end)
      candidate_with_log = Candidate.new(1, 1, [:b, :c], log_with_data, MockInterface)

      # older term despite higher index
      candidate_txn_id = {1, 5}

      {:ok, updated_candidate} =
        Candidate.vote_requested(candidate_with_log, 1, :peer_1, candidate_txn_id)

      assert updated_candidate.voted_for == nil
    end

    test "rejects candidate with same term but shorter log", %{candidate: _candidate} do
      {:ok, log_with_data} =
        InMemoryLog.new() |> Log.append_transactions({0, 0}, [{{2, 5}, "data"}])

      expect(MockInterface, :send_event, 2, fn _, {:request_vote, 1, {2, 5}} -> :ok end)
      expect(MockInterface, :timer, fn :election -> &mock_cancel/0 end)
      candidate_with_log = Candidate.new(1, 1, [:b, :c], log_with_data, MockInterface)

      # same term, lower index
      candidate_txn_id = {2, 3}

      {:ok, updated_candidate} =
        Candidate.vote_requested(candidate_with_log, 1, :peer_1, candidate_txn_id)

      assert updated_candidate.voted_for == nil
    end
  end

  describe "append_entries_received/6" do
    setup %{log: log} do
      expect(MockInterface, :send_event, 2, fn _, {:request_vote, 1, {0, 0}} -> :ok end)
      expect(MockInterface, :timer, fn :election -> &mock_cancel/0 end)

      candidate = Candidate.new(1, 1, [:b, :c], log, MockInterface)
      {:ok, candidate: candidate}
    end

    test "returns :become_follower when leader's term is greater", %{candidate: candidate} do
      :become_follower =
        Candidate.append_entries_received(candidate, 2, {0, 0}, [], {0, 0}, :b)
    end

    test "ignores append_entries when leader's term is less than candidate's", %{
      candidate: candidate
    } do
      {:ok, updated_candidate} =
        Candidate.append_entries_received(candidate, 0, {0, 0}, [], {0, 0}, :b)

      assert updated_candidate == candidate
    end
  end

  describe "add_transaction/2" do
    test "returns error as candidates cannot add transactions", %{log: log} do
      expect(MockInterface, :send_event, 2, fn _, {:request_vote, 1, {0, 0}} -> :ok end)
      expect(MockInterface, :timer, fn :election -> &mock_cancel/0 end)
      candidate = Candidate.new(1, 1, [:b, :c], log, MockInterface)

      assert {:error, :not_leader} = Candidate.add_transaction(candidate, "some_data")
    end
  end

  describe "append_entries_ack_received/4" do
    setup %{log: log} do
      expect(MockInterface, :send_event, 2, fn _, {:request_vote, 1, {0, 0}} -> :ok end)
      expect(MockInterface, :timer, fn :election -> &mock_cancel/0 end)
      candidate = Candidate.new(1, 1, [:b, :c], log, MockInterface)
      {:ok, candidate: candidate}
    end

    test "becomes follower when term is higher", %{candidate: candidate} do
      assert :become_follower =
               Candidate.append_entries_ack_received(candidate, 2, {1, 1}, :peer_1)
    end

    test "ignores when term is lower", %{candidate: candidate} do
      assert {:ok, _} = Candidate.append_entries_ack_received(candidate, 0, {1, 1}, :peer_1)
    end
  end

  describe "timer_ticked/2" do
    setup %{log: log} do
      expect(MockInterface, :send_event, 2, fn _, {:request_vote, 1, {0, 0}} -> :ok end)
      expect(MockInterface, :timer, fn :election -> &mock_cancel/0 end)
      candidate = Candidate.new(1, 1, [:b, :c], log, MockInterface)
      {:ok, candidate: candidate}
    end

    test "restarts election on election timeout", %{candidate: candidate} do
      expect(MockInterface, :send_event, 2, fn _, {:request_vote, 1, {0, 0}} -> :ok end)
      expect(MockInterface, :timer, fn :election -> &mock_cancel/0 end)

      {:ok, updated_candidate} = Candidate.timer_ticked(candidate, :election)

      assert updated_candidate.votes == []
    end

    test "ignores non-election timers", %{candidate: candidate} do
      assert {:ok, _} = Candidate.timer_ticked(candidate, :heartbeat)
    end
  end

  describe "vote handling edge cases" do
    test "ignores duplicate votes from same peer", %{log: log} do
      expect(MockInterface, :send_event, 2, fn _, {:request_vote, 1, {0, 0}} -> :ok end)
      expect(MockInterface, :timer, fn :election -> &mock_cancel/0 end)
      candidate = Candidate.new(1, 2, [:b, :c], log, MockInterface)

      {:ok, candidate_with_vote} = Candidate.vote_received(candidate, 1, :b)
      assert candidate_with_vote.votes == [:b]

      {:ok, candidate_with_duplicate} = Candidate.vote_received(candidate_with_vote, 1, :b)
      assert candidate_with_duplicate.votes == [:b]
    end

    test "ignores votes with lower term", %{log: log} do
      expect(MockInterface, :send_event, 2, fn _, {:request_vote, 1, {0, 0}} -> :ok end)
      expect(MockInterface, :timer, fn :election -> &mock_cancel/0 end)
      candidate = Candidate.new(1, 2, [:b, :c], log, MockInterface)

      {:ok, updated_candidate} = Candidate.vote_received(candidate, 0, :b)
      assert updated_candidate.votes == []
    end
  end

  describe "vote_requested when already voted" do
    test "does not vote again when already voted for another candidate", %{log: log} do
      expect(MockInterface, :send_event, 2, fn _, {:request_vote, 1, {0, 0}} -> :ok end)
      expect(MockInterface, :timer, fn :election -> &mock_cancel/0 end)
      candidate = Candidate.new(1, 1, [:b, :c], log, MockInterface)

      expect(MockInterface, :send_event, fn :peer_1, {:vote, 1} -> :ok end)
      {:ok, candidate_with_vote} = Candidate.vote_requested(candidate, 1, :peer_1, {1, 1})
      assert candidate_with_vote.voted_for == :peer_1

      {:ok, candidate_unchanged} =
        Candidate.vote_requested(candidate_with_vote, 1, :peer_2, {1, 2})

      assert candidate_unchanged.voted_for == :peer_1
    end
  end
end
