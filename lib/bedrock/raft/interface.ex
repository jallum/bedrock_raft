defmodule Bedrock.Raft.Interface do
  @moduledoc """
  The `Bedrock.Raft.Interface` module defines the public interface for
  interacting with the Raft consensus system.

  Implementations of this behaviour provides functions that the Raft consensus
  system needs in order to communicate, set timers and handle events. This
  allows the system to be used in a variety of contexts, and gives the user full
  control over how messages are sent and received.
  """
  alias Bedrock.Raft

  defmacro __using__(_opts) do
    quote do
      @behaviour Bedrock.Raft.Interface
    end
  end

  @type cancel_timer_fn :: (-> :ok)

  @callback heartbeat_ms() :: non_neg_integer()

  @callback timestamp_in_ms() :: non_neg_integer()

  @doc """
  An unhandled event has occurred.
  """
  @callback ignored_event(event :: any(), from :: Raft.peer() | :timer) :: :ok

  @doc """
  Set the leader of the Raft cluster.
  """
  @callback leadership_changed(Raft.leadership()) :: :ok

  @doc """
  Set a timer for a Raft peer. It's expected that the protocol will be notified
  when the timer expires.
  """
  @callback timer(:election | :heartbeat) :: cancel_timer_fn()

  @doc """
  Send an event to a Raft peer.
  """
  @callback send_event(to :: Raft.peer(), event :: any()) :: :ok

  @doc """
  Signal that a consensus has been reached up to the given transaction by the
  quorum of Raft peers.
  """
  @callback consensus_reached(Raft.Log.t(), Raft.transaction_id(), :latest | :behind) :: :ok

  @doc """
  Called when a leader detects quorum loss. The implementation decides whether
  to step down (:step_down) or continue as leader (:continue). This allows
  different policies:
  - Two-node clusters: typically :step_down when follower is lost
  - Larger clusters: typically :continue (standard Raft behavior)
  - Custom policies based on application requirements
  """
  @callback quorum_lost(
              active_followers :: non_neg_integer(),
              total_followers :: non_neg_integer(),
              term :: Raft.election_term()
            ) :: :step_down | :continue
end
