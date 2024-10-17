defmodule Bedrock.RaftTest do
  @moduledoc false
  use ExUnit.Case, async: true

  alias Bedrock.Raft
  alias Bedrock.Raft.Log
  alias Bedrock.Raft.Log.InMemoryLog
  alias Bedrock.Raft.Mode.Leader
  alias Bedrock.Raft.Mode.Follower
  alias Bedrock.Raft.Mode.Candidate

  import Mox
  alias Bedrock.Raft.MockInterface

  setup [:verify_on_exit!, :fix_stubs]

  def mock_timer_cancel, do: :ok

  def fix_stubs(context) do
    stub(MockInterface, :heartbeat_ms, fn -> 0 end)
    {:ok, context}
  end

  describe "Raft can be constructed" do
    test "when creating an instance with no other nodes, we start out as a leader" do
      expect(MockInterface, :timer, fn :heartbeat -> &mock_timer_cancel/0 end)
      expect(MockInterface, :leadership_changed, fn {:a, 0} -> :ok end)

      p = Raft.new(:a, [], InMemoryLog.new(), MockInterface)

      assert %Raft{
               me: :a,
               mode: %Leader{
                 nodes: [],
                 quorum: 0,
                 term: 0
               },
               nodes: [],
               quorum: 0
             } = p

      assert :a = Raft.me(p)
      assert {:a, 0} = Raft.leadership(p)
      assert Raft.am_i_the_leader?(p)
      assert [:a] = Raft.known_nodes(p)
    end

    test "when creating an instance with other nodes, we start out as a follower" do
      expect(MockInterface, :timer, fn :election -> &mock_timer_cancel/0 end)

      p = Raft.new(:a, [:b], InMemoryLog.new(), MockInterface)

      assert %Raft{
               me: :a,
               mode: %Follower{
                 term: 0,
                 leader: :undecided,
                 voted_for: nil,
                 cancel_timer_fn: cancel_timer_fn
               },
               nodes: [:b],
               quorum: 1
             } = p

      assert cancel_timer_fn == (&mock_timer_cancel/0)

      assert :a = Raft.me(p)
      assert {:undecided, 0} = Raft.leadership(p)
      refute Raft.am_i_the_leader?(p)
      assert [:a, :b] = Raft.known_nodes(p)
    end

    test "when creating an instance with more than zero other nodes, we start out as a follower" do
      expect(MockInterface, :timer, fn :election -> &mock_timer_cancel/0 end)

      p = Raft.new(:a, [:b], InMemoryLog.new(), MockInterface)

      assert %Raft{
               me: :a,
               mode: %Follower{
                 term: 0,
                 leader: :undecided,
                 voted_for: nil,
                 cancel_timer_fn: cancel_timer_fn
               },
               nodes: [:b],
               quorum: 1
             } = p

      assert cancel_timer_fn == (&mock_timer_cancel/0)

      assert :a = Raft.me(p)
      assert {:undecided, 0} = Raft.leadership(p)
      refute Raft.am_i_the_leader?(p)
      assert [:a, :b] = Raft.known_nodes(p)
    end
  end

  describe "Raft elections" do
    test "A happy-path election between three nodes that we started" do
      t0 = {0, 0}

      expect(MockInterface, :timer, fn :election -> &mock_timer_cancel/0 end)

      p = Raft.new(:a, [:b, :c], InMemoryLog.new(), MockInterface)

      # We haven't yet heard from any other nodes, so there's definitely no
      # leader.
      assert {:undecided, 0} = Raft.leadership(p)
      refute Raft.am_i_the_leader?(p)

      # Assuming that node :a's election timer expires, it will become a
      # candidate and start an election for term 2. It will send a request_vote
      # message to :b and :c.
      expect(MockInterface, :timer, fn :election -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :b, {:request_vote, 1, ^t0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:request_vote, 1, ^t0} -> :ok end)

      p = Raft.handle_event(p, :election, :timer)

      assert %Raft{
               me: :a,
               mode: %Candidate{
                 term: 1,
                 quorum: 1,
                 nodes: [:b, :c],
                 votes: []
               },
               nodes: [:b, :c],
               quorum: 1
             } = p

      # While we are a candidate, there's definitely no leader.
      assert {:undecided, 1} = Raft.leadership(p)
      refute Raft.am_i_the_leader?(p)

      # Assuming that node :b receives the request_vote message first, it will
      # respond with a vote for term 2. With one vote, we reach the quorum and
      # become leader.
      expect(MockInterface, :timer, fn :heartbeat -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :b, {:append_entries, 1, ^t0, [], ^t0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:append_entries, 1, ^t0, [], ^t0} -> :ok end)
      expect(MockInterface, :leadership_changed, fn {:a, 1} -> :ok end)

      p = Raft.handle_event(p, {:vote, 1}, :b)

      # After we've won the election, we should be the leader.

      verify!()
      assert {:a, 1} = Raft.leadership(p)
      assert Raft.am_i_the_leader?(p)

      assert %Raft{
               me: :a,
               mode: %Leader{
                 nodes: [:b, :c],
                 quorum: 1,
                 term: 1
               },
               nodes: [:b, :c],
               quorum: 1
             } = p

      # Assuming that node :c also responds to the request_vote, we've already
      # reached the quorum and become leader. We'll ignore the vote.

      p = Raft.handle_event(p, {:vote, 1}, :c)

      verify!()

      assert %Raft{
               me: :a,
               mode: %Leader{
                 nodes: [:b, :c],
                 quorum: 1,
                 term: 1
               },
               nodes: [:b, :c],
               quorum: 1
             } = p

      # Assuming that node :c receives the ping and responds with a pong,
      # ensure that it is recorded properly.
      p = Raft.handle_event(p, {:append_entries_ack, 1, {0, 0}}, :c)

      verify!()

      assert %Raft{
               me: :a,
               mode: %Leader{
                 nodes: [:b, :c],
                 quorum: 1,
                 term: 1
               },
               nodes: [:b, :c],
               quorum: 1
             } = p

      # At the next heartbeat timer, assuming that node :c receives the ping and
      # responds with a pong, ensure that it is recorded properly.
      expect(MockInterface, :timer, fn :heartbeat -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :b, {:append_entries, 1, ^t0, [], ^t0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:append_entries, 1, ^t0, [], ^t0} -> :ok end)

      p = Raft.handle_event(p, :heartbeat, :timer)

      verify!()

      assert %Raft{
               me: :a,
               mode: %Leader{
                 nodes: [:b, :c],
                 quorum: 1,
                 term: 1
               },
               nodes: [:b, :c],
               quorum: 1
             } = p
    end

    test "A happy-path election between three nodes that we vote in" do
      log = InMemoryLog.new()

      t0 = Log.initial_transaction_id(log)

      expect(MockInterface, :timer, fn :election -> &mock_timer_cancel/0 end)

      p = Raft.new(:a, [:b, :c], log, MockInterface)

      verify!()

      assert %Raft{
               me: :a,
               mode: %Follower{
                 term: 0,
                 leader: :undecided,
                 voted_for: nil
               },
               nodes: [:b, :c],
               quorum: 1
             } = p

      # Node :c calls an election for term 1, and sends a request_vote message

      expect(MockInterface, :timer, fn :election -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :c, {:vote, 1} -> :ok end)

      p = Raft.handle_event(p, {:request_vote, 1, {0, 1}}, :c)

      verify!()

      assert %Raft{
               me: :a,
               mode: %Follower{
                 term: 1,
                 leader: :undecided,
                 voted_for: :c
               },
               nodes: [:b, :c],
               quorum: 1
             } = p

      # Node :c calls to inform that the election has been decided for term 1,
      # and that it has been elected.

      expect(MockInterface, :timer, fn :election -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :c, {:append_entries_ack, 1, ^t0} -> :ok end)
      expect(MockInterface, :leadership_changed, fn {:c, 1} -> :ok end)

      p = Raft.handle_event(p, {:append_entries, 1, t0, [], t0}, :c)

      verify!()

      assert %Raft{
               me: :a,
               mode: %Follower{
                 term: 1,
                 leader: :c,
                 voted_for: :c
               },
               nodes: [:b, :c],
               quorum: 1
             } = p

      assert {:c, 1} = Raft.leadership(p)
    end

    test "After becoming a candidate, if the election timer expires then a new election is started" do
      t0 = {0, 0}

      expect(MockInterface, :timer, fn :election -> &mock_timer_cancel/0 end)

      p = Raft.new(:a, [:b, :c], InMemoryLog.new(), MockInterface)

      verify!()

      # Timer elapses the first time, so we start an election for term 1

      expect(MockInterface, :timer, fn :election -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :b, {:request_vote, 1, ^t0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:request_vote, 1, ^t0} -> :ok end)

      p = Raft.handle_event(p, :election, :timer)

      verify!()

      assert %Raft{
               me: :a,
               mode: %Candidate{
                 term: 1,
                 quorum: 1,
                 nodes: [:b, :c],
                 votes: []
               },
               nodes: [:b, :c],
               quorum: 1
             } = p

      # Timer elapses the second time with no votes, so we start another
      # election for term 1

      expect(MockInterface, :timer, fn :election -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :b, {:request_vote, 1, ^t0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:request_vote, 1, ^t0} -> :ok end)

      p = Raft.handle_event(p, :election, :timer)

      verify!()

      assert %Raft{
               me: :a,
               mode: %Candidate{
                 term: 1,
                 quorum: 1,
                 nodes: [:b, :c],
                 votes: []
               },
               nodes: [:b, :c],
               quorum: 1
             } = p
    end

    test "In a three node cluster, as a candidate we receive a vote from a follower in an older term" do
      t0 = {0, 0}

      expect(MockInterface, :timer, fn :election -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :b, {:request_vote, 1, ^t0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:request_vote, 1, ^t0} -> :ok end)
      expect(MockInterface, :timer, fn :election -> &mock_timer_cancel/0 end)

      p =
        Raft.new(:a, [:b, :c], InMemoryLog.new(), MockInterface)
        |> Raft.handle_event(:election, :timer)

      verify!()
      assert %Raft{mode: %Candidate{}} = p
      assert {:undecided, 1} = Raft.leadership(p)

      # Packets were dropped, :a became separated, an election was held and a
      # new leader, :c, was elected. After the split was healed, :a receives a
      # heartbeat from the new leader in term 3.

      p = p |> Raft.handle_event({:vote, 0}, :c)

      verify!()
      assert {:undecided, 1} = Raft.leadership(p)
    end

    test "In a three node cluster, as a candidate we receive a ping from a leader in the same term" do
      t0 = {0, 0}

      expect(MockInterface, :timer, fn :election -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :b, {:request_vote, 1, ^t0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:request_vote, 1, ^t0} -> :ok end)
      expect(MockInterface, :timer, fn :election -> &mock_timer_cancel/0 end)

      p =
        Raft.new(:a, [:b, :c], InMemoryLog.new(), MockInterface)
        |> Raft.handle_event(:election, :timer)

      verify!()
      assert %Raft{mode: %Candidate{}} = p
      assert {:undecided, 1} = Raft.leadership(p)

      # Packets were dropped, :a became separated, an election was held and a
      # new leader, :c, was elected. After the split was healed, :a receives a
      # heartbeat from the new leader in term 3.

      expect(MockInterface, :timer, fn :election -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :c, {:append_entries_ack, 2, ^t0} -> :ok end)
      expect(MockInterface, :leadership_changed, fn {:c, 2} -> :ok end)

      p = p |> Raft.handle_event({:append_entries, 2, t0, [], t0}, :c)

      verify!()
      assert {:c, 2} = Raft.leadership(p)
    end

    test "In a three node cluster, as a candidate we receive a ping from a leader in an older term" do
      t0 = {0, 0}

      expect(MockInterface, :timer, fn :election -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :b, {:request_vote, 1, ^t0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:request_vote, 1, ^t0} -> :ok end)
      expect(MockInterface, :timer, fn :election -> &mock_timer_cancel/0 end)

      p =
        Raft.new(:a, [:b, :c], InMemoryLog.new(), MockInterface)
        |> Raft.handle_event(:election, :timer)

      verify!()
      assert %Raft{mode: %Candidate{}} = p
      assert {:undecided, 1} = Raft.leadership(p)

      # Packets were dropped, :a became separated, an election was held and a
      # new leader, :c, was elected. After the split was healed, :a receives a
      # heartbeat from the leader of term 1.

      p = p |> Raft.handle_event({:append_entries, 0, t0, [], t0}, :c)

      verify!()
      assert {:undecided, 1} = Raft.leadership(p)
    end

    test "In a three node cluster, after winning a successful election during a network split, a new leader was elected with no other missed transactions" do
      t0 = {0, 0}
      t1 = {2, 0}

      expect(MockInterface, :timer, fn :election -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :b, {:request_vote, 1, ^t0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:request_vote, 1, ^t0} -> :ok end)
      expect(MockInterface, :timer, fn :election -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :b, {:append_entries, 1, ^t0, [], ^t0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:append_entries, 1, ^t0, [], ^t0} -> :ok end)
      expect(MockInterface, :timer, fn :heartbeat -> &mock_timer_cancel/0 end)
      expect(MockInterface, :leadership_changed, fn {:a, 1} -> :ok end)
      expect(MockInterface, :timer, fn :heartbeat -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :b, {:append_entries, 1, ^t0, [], ^t0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:append_entries, 1, ^t0, [], ^t0} -> :ok end)

      p =
        Raft.new(:a, [:b, :c], InMemoryLog.new(), MockInterface)
        |> Raft.handle_event(:election, :timer)
        |> Raft.handle_event({:vote, 1}, :c)
        |> Raft.handle_event({:vote, 1}, :b)
        |> Raft.handle_event({:append_entries_ack, 1, t0}, :c)
        |> Raft.handle_event({:append_entries_ack, 1, t0}, :b)
        |> Raft.handle_event(:heartbeat, :timer)

      verify!()

      assert {:a, 1} = Raft.leadership(p)

      # Packets were dropped, :a became separated, an election was held and a
      # new leader, :c, was elected. After the split was healed, :a receives a
      # heartbeat from the new leader in term 2.

      expect(MockInterface, :timer, fn :election -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :c, {:append_entries_ack, 2, ^t0} -> :ok end)
      expect(MockInterface, :leadership_changed, fn {:c, 2} -> :ok end)

      p = p |> Raft.handle_event({:append_entries, 2, t1, [{t1, :data1}], t1}, :c)

      verify!()
      assert {:c, 2} = Raft.leadership(p)
    end

    test "In a three node cluster, after winning a successful election during a network split, a new leader was elected with new transactions that we missed" do
      log = InMemoryLog.new(:tuple)

      t0 = Log.initial_transaction_id(log)

      expect(MockInterface, :timer, fn :election -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :b, {:request_vote, 1, ^t0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:request_vote, 1, ^t0} -> :ok end)
      expect(MockInterface, :timer, fn :election -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :b, {:append_entries, 1, ^t0, [], ^t0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:append_entries, 1, ^t0, [], ^t0} -> :ok end)
      expect(MockInterface, :timer, fn :heartbeat -> &mock_timer_cancel/0 end)
      expect(MockInterface, :leadership_changed, fn {:a, 1} -> :ok end)
      expect(MockInterface, :timer, fn :heartbeat -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :b, {:append_entries, 1, ^t0, [], ^t0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:append_entries, 1, ^t0, [], ^t0} -> :ok end)

      p =
        Raft.new(:a, [:b, :c], log, MockInterface)
        |> Raft.handle_event(:election, :timer)
        |> Raft.handle_event({:vote, 1}, :c)
        |> Raft.handle_event({:vote, 1}, :b)
        |> Raft.handle_event({:append_entries_ack, 1, t0}, :c)
        |> Raft.handle_event({:append_entries_ack, 1, t0}, :b)
        |> Raft.handle_event(:heartbeat, :timer)

      verify!()
      assert {:a, 1} = Raft.leadership(p)

      # Packets were dropped, :a became separated, an election was held and a
      # new leader, :c, was elected. After the split was healed, :a receives a
      # heartbeat from the new leader in term 2, but is missing transactions.
      # :a will request the missing transactions from the new leader.

      expect(MockInterface, :timer, fn :election -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :c, {:append_entries_ack, 2, ^t0} -> :ok end)
      expect(MockInterface, :leadership_changed, fn {:c, 2} -> :ok end)

      {:ok, p, t1} = Raft.next_transaction_id(p)
      {:ok, p, t2} = Raft.next_transaction_id(p)
      p = p |> Raft.handle_event({:append_entries, 2, t1, [{t2, :data1}], t1}, :c)

      verify!()

      # When :c sends the missing transactions, we add them to our log and note
      # that fact by sending an ack. We reset our heartbeat timer. Since
      # we've received all the transactions up to t1, we can now signal
      # consensus up to that point.

      expect(MockInterface, :timer, fn :election -> &mock_timer_cancel/0 end)
      expect(MockInterface, :consensus_reached, fn _, ^t1 -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:append_entries_ack, 2, ^t2} -> :ok end)
      p = p |> Raft.handle_event({:append_entries, 2, t0, [{t0, :data1}, {t2, :data1}], t1}, :c)

      verify!()

      # When :c receives the ack, it will send us a new append entries message
      # (that in this case, does not contain any new transactions) that tells
      # us that consensus has been reached up to t2. We will reset our heartbeat
      # timer and signal consensus up to t2.

      expect(MockInterface, :timer, fn :election -> &mock_timer_cancel/0 end)
      expect(MockInterface, :consensus_reached, fn _, ^t2 -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:append_entries_ack, 2, ^t2} -> :ok end)
      p = p |> Raft.handle_event({:append_entries, 2, t2, [], t2}, :c)

      verify!()
      assert {:c, 2} = Raft.leadership(p)
    end
  end

  describe "Raft quorum failures" do
    test "A three node cluster where we are the leader, and quorum fails" do
      log = InMemoryLog.new()

      t0 = Log.initial_transaction_id(log)

      expect(MockInterface, :timer, fn :election -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :b, {:request_vote, 1, ^t0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:request_vote, 1, ^t0} -> :ok end)
      expect(MockInterface, :timer, fn :election -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :b, {:append_entries, 1, ^t0, [], ^t0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:append_entries, 1, ^t0, [], ^t0} -> :ok end)
      expect(MockInterface, :timer, fn :heartbeat -> &mock_timer_cancel/0 end)
      expect(MockInterface, :leadership_changed, fn {:a, 1} -> :ok end)

      p =
        Raft.new(:a, [:b, :c], log, MockInterface)
        |> Raft.handle_event(:election, :timer)
        |> Raft.handle_event({:vote, 1}, :c)
        |> Raft.handle_event({:vote, 1}, :b)
        |> Raft.handle_event({:append_entries_ack, 1, t0}, :c)
        |> Raft.handle_event({:append_entries_ack, 1, t0}, :b)

      verify!()

      assert %Raft{
               me: :a,
               mode: %Leader{
                 nodes: [:b, :c],
                 quorum: 1,
                 term: 1
               },
               nodes: [:b, :c],
               quorum: 1
             } = p

      assert {:a, 1} = Raft.leadership(p)

      # some time goes by, and we don't hear back from either :b or :c...

      expect(MockInterface, :timer, fn :heartbeat -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :b, {:append_entries, 1, ^t0, [], ^t0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:append_entries, 1, ^t0, [], ^t0} -> :ok end)

      p = p |> Raft.handle_event(:heartbeat, :timer)

      verify!()

      # some time goes by, and we don't hear back from either :b or :c...

      expect(MockInterface, :timer, fn :heartbeat -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :b, {:append_entries, 1, ^t0, [], ^t0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:append_entries, 1, ^t0, [], ^t0} -> :ok end)

      p = p |> Raft.handle_event(:heartbeat, :timer)

      verify!()

      # some time goes by, and we don't hear back from either :b or :c...

      expect(MockInterface, :timer, fn :heartbeat -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :b, {:append_entries, 1, ^t0, [], ^t0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:append_entries, 1, ^t0, [], ^t0} -> :ok end)

      p = p |> Raft.handle_event(:heartbeat, :timer)

      verify!()

      # some time goes by, and we don't hear back from either :b or :c...

      expect(MockInterface, :timer, fn :heartbeat -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :b, {:append_entries, 1, ^t0, [], ^t0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:append_entries, 1, ^t0, [], ^t0} -> :ok end)

      p = p |> Raft.handle_event(:heartbeat, :timer)

      verify!()

      # some time goes by, and we don't hear back from either :b or :c...

      expect(MockInterface, :timer, fn :heartbeat -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :b, {:append_entries, 1, ^t0, [], ^t0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:append_entries, 1, ^t0, [], ^t0} -> :ok end)

      _p = p |> Raft.handle_event(:heartbeat, :timer)

      verify!()
    end
  end

  describe "Raft log interaction" do
    test "A three node cluster where we become the leader, and then we append an entry" do
      log = InMemoryLog.new()

      t0 = Log.initial_transaction_id(log)

      # We start off as a follower. Our election timer expires and we start an
      # election for term 1. We send a request_vote message to :b and :c.

      expect(MockInterface, :timer, fn :election -> &mock_timer_cancel/0 end)
      expect(MockInterface, :timer, fn :election -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :b, {:request_vote, 1, ^t0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:request_vote, 1, ^t0} -> :ok end)

      p =
        Raft.new(:a, [:b, :c], log, MockInterface)
        |> Raft.handle_event(:election, :timer)

      verify!()
      refute p |> Raft.am_i_the_leader?()

      # We receive a vote from :b and :c, and we become the leader. We send an
      # append_entries message to :b and :c.

      expect(MockInterface, :timer, fn :heartbeat -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :b, {:append_entries, 1, ^t0, [], ^t0} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:append_entries, 1, ^t0, [], ^t0} -> :ok end)
      expect(MockInterface, :leadership_changed, fn {:a, 1} -> :ok end)

      p =
        p
        |> Raft.handle_event({:vote, 1}, :c)
        |> Raft.handle_event({:vote, 1}, :b)

      verify!()
      assert p |> Raft.am_i_the_leader?()

      # We receive an ack from :b and :c, for t0 and we reset our heartbeat timer.

      p =
        p
        |> Raft.handle_event({:append_entries_ack, 1, t0}, :b)
        |> Raft.handle_event({:append_entries_ack, 1, t0}, :c)

      verify!()
      assert p |> Raft.am_i_the_leader?()

      # We add a transaction (t1) to our log, and send it to :b and :c.

      expect(MockInterface, :send_event, fn :b, {:append_entries, 1, ^t0, [{t1, :data1}], ^t0} ->
        send(self(), t1)
        :ok
      end)

      expect(MockInterface, :send_event, fn :c, {:append_entries, 1, ^t0, [{t1, :data1}], ^t0} ->
        send(self(), t1)
        :ok
      end)

      assert {:ok, p, t1} = Raft.add_transaction(p, :data1)
      verify!()
      assert_receive ^t1
      assert_receive ^t1

      # We add another transaction (t2) to our log, and send it to :b and :c.

      expect(MockInterface, :send_event, fn :b, {:append_entries, 1, ^t1, [{t2, :data2}], ^t0} ->
        send(self(), t2)
        :ok
      end)

      expect(MockInterface, :send_event, fn :c, {:append_entries, 1, ^t1, [{t2, :data2}], ^t0} ->
        send(self(), t2)
        :ok
      end)

      assert {:ok, p, t2} = Raft.add_transaction(p, :data2)
      verify!()
      assert_receive ^t2
      assert_receive ^t2

      # Our heartbeat timer expires, and we send out a heartbeat to :b and :c.
      # We note that the newest committed transaction is still t0

      expect(MockInterface, :timer, fn :heartbeat -> &mock_timer_cancel/0 end)

      expect(MockInterface, :send_event, fn :b, {:append_entries, 1, ^t2, [], ^t0} ->
        :ok
      end)

      expect(MockInterface, :send_event, fn :c, {:append_entries, 1, ^t2, [], ^t0} ->
        :ok
      end)

      p = p |> Raft.handle_event(:heartbeat, :timer)

      verify!()
      assert [{t1, :data1}, {t2, :data2}] == p |> Raft.log() |> Log.transactions_from(t0, :newest)
      assert t2 == p |> Raft.log() |> Log.newest_transaction_id()
      assert t0 == p |> Raft.log() |> Log.newest_safe_transaction_id()

      # :b acknowledges the heartbeat saying that it has received up to t2, and
      # we make a note of that. since we *also* have a copy of t2, two nodes
      # constitutes a quorum and we can decide that consensus has been reached
      # up to t2. we then send out a new heartbeat to both :b and :c to let them
      # know that consensus has been reached up to t2.

      expect(MockInterface, :consensus_reached, fn _, ^t2 -> :ok end)
      expect(MockInterface, :send_event, fn :b, {:append_entries, 1, ^t2, [], ^t2} -> :ok end)
      expect(MockInterface, :send_event, fn :c, {:append_entries, 1, ^t2, [], ^t2} -> :ok end)

      p = p |> Raft.handle_event({:append_entries_ack, 1, t2}, :b)

      verify!()
      assert t2 == p |> Raft.log() |> Log.newest_transaction_id()
      assert t2 == p |> Raft.log() |> Log.newest_safe_transaction_id()

      # :c now acknowledges the heartbeat saying that it has received up to t2
      # as well, and we make a note of that.

      p = p |> Raft.handle_event({:append_entries_ack, 1, t2}, :c)

      verify!()
      assert t2 == p |> Raft.log() |> Log.newest_transaction_id()
      assert t2 == p |> Raft.log() |> Log.newest_safe_transaction_id()
    end

    test "A three node cluster where we are a follower, where we start off at nothing and need to catch up" do
      log = InMemoryLog.new()
      t0 = Log.initial_transaction_id(log)

      t1 = {2, 0}
      t2 = {2, 1}

      expect(MockInterface, :timer, fn :election -> &mock_timer_cancel/0 end)

      p = Raft.new(:a, [:b, :c], log, MockInterface)

      verify!()

      # some time passes, and we hear a from an active leader, trying to get us
      # to append past the transactions we have in our log. we'll need to reply
      # with the latest transaction we have.

      expect(MockInterface, :timer, fn :election -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :c, {:append_entries_ack, 1, ^t0} -> :ok end)
      expect(MockInterface, :leadership_changed, fn {:c, 1} -> :ok end)

      p = p |> Raft.handle_event({:append_entries, 1, t1, [{t2, :data1}], t1}, :c)

      verify!()

      # the leader responds with a new set of transactions, beginning where we
      # left off, and we append them to our log. we also acknowledge the leader.
      expect(MockInterface, :timer, fn :election -> &mock_timer_cancel/0 end)
      expect(MockInterface, :send_event, fn :c, {:append_entries_ack, 1, ^t2} -> :ok end)
      expect(MockInterface, :consensus_reached, fn _, ^t2 -> :ok end)

      p = p |> Raft.handle_event({:append_entries, 1, t0, [{t1, :data1}, {t2, :data2}], t2}, :c)

      verify!()

      # we should have the transactions in our log, and the latest transaction
      # should be the one we just appended. the latest safe transaction should
      # be the one we just appended as well.
      assert [{t1, :data1}, {t2, :data2}] == p |> Raft.log() |> Log.transactions_from(t0, :newest)
      assert t2 == p |> Raft.log() |> Log.newest_transaction_id()
      assert t2 == p |> Raft.log() |> Log.newest_safe_transaction_id()
    end
  end
end
