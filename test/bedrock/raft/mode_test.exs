defmodule Bedrock.Raft.ModeTest do
  @moduledoc false
  use ExUnit.Case, async: true

  test "module loads and defines behaviour" do
    assert Code.ensure_loaded?(Bedrock.Raft.Mode)
    assert is_atom(Bedrock.Raft.Mode)
    assert function_exported?(Bedrock.Raft.Mode, :__info__, 1)
  end
end
