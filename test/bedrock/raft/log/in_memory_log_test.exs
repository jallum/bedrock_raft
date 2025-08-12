defmodule Bedrock.Raft.Log.InMemoryLogTest do
  use ExUnit.Case, async: true

  alias Bedrock.Raft.Log.BinaryInMemoryLog
  alias Bedrock.Raft.Log.InMemoryLog
  alias Bedrock.Raft.Log.TupleInMemoryLog

  describe "new/1" do
    test "creates a new log with tuple format" do
      assert %TupleInMemoryLog{} = InMemoryLog.new(:tuple)
    end

    test "creates a new log with binary format" do
      assert %BinaryInMemoryLog{} = InMemoryLog.new(:binary)
    end
  end
end
