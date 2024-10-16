defmodule Bedrock.Raft.Mode.CandidateTest do
  @moduledoc false
  use ExUnit.Case, async: true
  alias Bedrock.Raft.Mode.Candidate
  alias Bedrock.Raft.Log.InMemoryLog
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
      nodes = [:b, :c]
      expect(MockInterface, :timer, fn :election -> fn -> :ok end end)
      expect(MockInterface, :send_event, 2, fn _, {:request_vote, 1, {0, 0}} -> :ok end)

      candidate = Candidate.new(1, 1, nodes, log, MockInterface)

      assert %Candidate{
               term: 1,
               quorum: 1,
               nodes: ^nodes,
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
end
