defmodule Swarm.Tracker do
  @behaviour Phoenix.Tracker

  def start_link(opts \\ []) do
    pubsub_server = Keyword.fetch!(Application.get_env(:swarm, :pubsub), :name)
    full_opts = Keyword.merge([name: __MODULE__, pubsub_server: pubsub_server], opts)
    GenServer.start_link(Phoenix.Tracker, [__MODULE__, full_opts, full_opts], [name: __MODULE__])
  end

  @doc false
  def init(opts) do
    server = Keyword.fetch!(opts, :pubsub_server)
    {:ok, %{pubsub_server: server}}
  end


  def add_service(type) do
    add_service(type, self)
  end
  def add_service(type, pid) do
    case Phoenix.Tracker.track(__MODULE__, pid, type, pid, %{node: node()}) do
      {:ok, _} -> :ok
      err -> err
    end
  end

  def remove_service(type, pid) do
    Phoenix.Tracker.untrack(__MODULE__, pid, type, pid)
  end

  def get_services(type) do
    Phoenix.Tracker.list(__MODULE__, type)
  end

  def find_service(type, key) do
    case :hash_ring.find_node(type, :erlang.term_to_binary(key)) do
      {:ok, info} ->
        case :erlang.binary_to_term(info) do
          {host, pid} when is_pid(pid) ->
            {:ok, host, pid}
          _ ->
            {:error, :no_service_for_key}
        end
      err ->
        err
    end
  end

  def find_multi_service(count, type, key) do
    case :hash_ring.get_nodes(type, :erlang.term_to_binary(key), count) do
      {:ok, infolist} ->
        case Enum.map(infolist, &:erlang.binary_to_term/1) do
          list when is_list(list) ->
            list
          _ ->
            []
        end
      err ->
        err
    end
  end

  def cast(type, key, params) do
    case find_service(type, key) do
      {:ok, _node, pid} -> GenServer.cast(pid, params)
      _ -> {:error, :service_unavailable}
    end
  end

  def call(type, key, params, timeout \\ 5000) do
    case find_service(type, key) do
      {:ok, _node, pid} -> GenServer.call(pid, params, timeout)
      _ -> {:error, :service_unavailable}
    end
  end

  def multi_cast(count, type, key, params) do
    case find_multi_service(count, type, key) do
      [] -> {:error, :service_unavailable}
      servers ->
        servers
        |> Enum.each(fn {_node, pid} -> GenServer.cast(pid, params) end)
        {:ok, Enum.count(servers)}
    end
  end

  def multi_call(count, type, key, params, timeout \\ 5000) do
    case find_multi_service(count, type, key) do
      [] -> {:error, :service_unavailable}
      servers ->
        for {_node, pid} <- servers do
          Task.Supervisor.async_nolink(Swarm.TaskSupervisor, fn ->
            try do
              {:ok, pid, GenServer.call(pid, params, timeout)}
            catch
              :exit, reason -> {:error, pid, reason}
            end
          end)
      end
      |> Enum.map(&Task.await(&1, :infinity))
    end
  end

  @doc false
  def handle_diff(diff, state) do
    IO.inspect {Node.self(), diff}
    for {type, {joins, leaves}} <- diff do
      unless :hash_ring.has_ring(type) do
        :ok = :hash_ring.create_ring(type, 128)
      end
      for {pid, meta} <- leaves do
        service_info = :erlang.term_to_binary({meta.node, pid})
        unless Enum.any?(joins, fn {jpid, _meta} -> jpid == pid end) do
          :hash_ring.remove_node(type, service_info)
        end
        Phoenix.PubSub.direct_broadcast(node(), state.pubsub_server, type, {:leave, pid, meta})
      end
      for {pid, meta} <- joins do
        service_info = :erlang.term_to_binary({meta.node, pid})
        :hash_ring.add_node(type, service_info)
        Phoenix.PubSub.direct_broadcast(node(), state.pubsub_server, type, {:join, pid, meta})
      end
    end
    {:ok, state}
  end
end
