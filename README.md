# Bedrock Raft

[![Build Status](https://github.com/jallum/bedrock_raft/actions/workflows/elixir_ci.yaml/badge.svg)](https://github.com/jallum/bedrock_raft/actions/workflows/elixir_ci.yaml?branch=main)
[![Coverage Status](https://coveralls.io/repos/github/jallum/bedrock_raft/badge.svg?branch=main)](https://coveralls.io/github/jallum/bedrock_raft?branch=main)

An implementation of the RAFT consensus algorithm in Elixir that doesn't force a lot of opinions. You can bake the protocol into your own genservers, send messages and manage the logs how you like.

## Installation

```elixir
def deps do
  [
    {:bedrock_raft, "~> 0.9"}
  ]
end
```
