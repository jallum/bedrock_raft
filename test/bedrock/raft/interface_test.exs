defmodule Bedrock.Raft.InterfaceTest do
  @moduledoc false
  use ExUnit.Case, async: true

  test "__using__ macro adds behaviour" do
    defmodule TestInterface do
      use Bedrock.Raft.Interface
    end

    behaviours = TestInterface.__info__(:attributes)[:behaviour] || []
    assert Bedrock.Raft.Interface in behaviours
  end
end
