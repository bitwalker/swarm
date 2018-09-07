defmodule Swarm.Nodes do
  alias Porcelain.Process, as: Proc
  require Logger

  def start(name, config \\ nil) when is_atom(name) do
    Application.ensure_started(:porcelain)

    config_file =
      case System.get_env("SWARM_DEBUG") do
        "true" -> "sys_debug.config"
        _ -> "sys.config"
      end

    "swarm_master@" <> hostname = "#{Node.self()}"
    node_name = :"#{name}@#{hostname}"

    node_pid =
      spawn_link(fn ->
        Process.flag(:trap_exit, true)
        code_paths = :code.get_path()

        base_args = [
          "-noshell",
          "-connect_all false",
          "-hidden",
          "-sname #{node_name}",
          "-setcookie swarm_test",
          "-config #{Path.join(__DIR__, config_file)}",
          "-eval 'io:format(\"ok\", []).'"
        ]

        args =
          Enum.reduce(code_paths, Enum.join(base_args, " "), fn path, acc ->
            acc <> " -pa #{path}"
          end)

        _proc =
          %Proc{pid: pid} =
          Porcelain.spawn_shell("erl " <> args, in: :receive, out: {:send, self()})

        :ok = wait_until_started(node_name, pid)
        true = :net_kernel.hidden_connect_node(node_name)
        receive_loop(node_name, pid)
      end)

    :ok = block_until_nodeup(node_pid)

    case config do
      nil ->
        :ok

      _ ->
        for {k, v} <- config do
          :rpc.call(node, Application, :put_env, [:swarm, k, v])
        end
    end

    {:ok, _} = :rpc.call(node, Application, :ensure_all_started, [:elixir])
    {:ok, node_name, node_pid}
  end

  def stop(name) when is_atom(name) do
    :abcast = :rpc.eval_everywhere([name], :init, :stop, [])
  end

  defp block_until_nodeup(pid) do
    case GenServer.call(pid, :ready) do
      true ->
        :ok

      false ->
        block_until_nodeup(pid)
    end
  end

  defp wait_until_started(node_name, pid) do
    receive do
      {^pid, :data, :out, data} ->
        # IO.inspect {node_name, data}
        :ok

      {^pid, :result, %{status: status}} ->
        {:error, status}

      {:"$gen_call", from, :ready} ->
        GenServer.reply(from, false)
        wait_until_started(node_name, pid)
    end
  end

  defp receive_loop(node_name, pid) do
    receive do
      {^pid, :data, :out, data} ->
        case Application.get_env(:logger, :level, :warn) do
          l when l in [:debug, :info] ->
            IO.puts("#{node_name} =>\n" <> data)

          _ ->
            :ok
        end

        receive_loop(node_name, pid)

      {^pid, :result, %{status: status}} ->
        IO.inspect({:exit, node_name, status})

      {:EXIT, parent, reason} when parent == self() ->
        Process.exit(pid, reason)

      {:"$gen_call", from, :ready} ->
        GenServer.reply(from, true)
        receive_loop(node_name, pid)

      :die ->
        Process.exit(pid, :normal)
    end
  end
end
