defmodule Bedrock.Raft.Log.InMemoryLog do
  @moduledoc """
  An implementation of the transaction log that stores transactions in memory,
  and makes no guarantees about durability or persistence. This is useful for
  testing and development, but should not be used in production. IDs can be
  stored in either tuple or binary format.
  """
  alias Bedrock.Raft.Log.BinaryInMemoryLog
  alias Bedrock.Raft.Log.TupleInMemoryLog

  @doc """
  Create a new in-memory log. The `format` argument can be `:tuple` or
  `:binary`, and determines the format of the transaction ids used in the log.
  """
  @spec new(:tuple | :binary) :: BinaryInMemoryLog.t() | TupleInMemoryLog.t()
  def new(format \\ :tuple)
  def new(:binary), do: BinaryInMemoryLog.new()
  def new(:tuple), do: TupleInMemoryLog.new()
end
