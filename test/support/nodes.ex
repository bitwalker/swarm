defmodule Swarm.Nodes do
  def start(name, config \\ nil) when is_atom(name) do
    "swarm_master@" <> hostname = "#{Node.self}"
    node_name = :'#{name}@#{hostname}'
    node = case :net_adm.ping(node_name) do
      :pong ->
        node_name
      _ ->
        code_paths = :code.get_path()
        args = Enum.reduce(code_paths, "", fn path, acc ->
          acc <> " -pa #{path}"
        end)
        {:ok, node} = :slave.start_link('#{hostname}', name, String.to_charlist(args))
        node
    end
    case config do
      nil -> :ok
      _ ->
        for {k, v} <- config do
          :rpc.call(node, Application, :put_env, [:swarm, k, v])
        end
    end
    {:ok, _} = :rpc.call(node, Application, :ensure_all_started, [:elixir])
    node
  end

  def stop(name) when is_atom(name) do
    "swarm_master@" <> hostname = "#{Node.self}"
    :ok = :slave.stop(:'#{name}@#{hostname}')
  end
end
