defmodule Distable.Heartbeat do
  @moduledoc """
  This process is responsible for multicasting heartbeats containing
  this node's name, which is used by receiving nodes to establish a
  connection, and thus form a dynamic cluster of nodes. This process
  is also a receiver of those same heartbeats, and handles connecting
  to remote nodes.

  The heartbeat protocol uses UDP, on a configurable port and multicast
  address. By default, it uses port 45892 and multicast address 224.0.0.251.
  """
  use GenServer
  import Distable.Logger

  @default_port 45892
  @default_addr {0,0,0,0}
  @default_multicast_addr {230,0,0,251}

  def start_link() do
    cluster? = Application.get_env(:distable, :cluster, true)
    case cluster do
      true  -> GenServer.start_link(__MODULE__, [], name: __MODULE__)
      false -> :ignore
    end
  end

  def init(_) do
    opts = Application.get_all_env(:distable)
    port = Keyword.get(opts, :port, @default_port)
    ip = Keyword.get(opts, :ip_addr, @default_addr)
    multicast_addr = case Keyword.get(opts, :multicast_addr, @default_multicast_addr) do
                       {_a,_b,_c,_d} = ip -> ip
                       ip when is_binary(ip) ->
                         {:ok, addr} = :inet.parse_ipv4_address(ip)
                         addr
                     end
    {:ok, socket} = :gen_udp.open(port, [
          :binary,
          active: true,
          ip: ip,
          reuseaddr: true,
          broadcast: true,
          multicast_ttl: 4,
          multicast_loop: true,
          add_membership: {multicast_addr, {0,0,0,0}}
        ])
    Process.send_after(self(), :heartbeat, :rand.uniform(5_000))
    {:ok, {multicast_addr, port, socket}}
  end

  # Send stuttered heartbeats
  def handle_info(:heartbeat, {multicast_addr, port, socket} = state) do
    :ok = :gen_udp.send(socket, multicast_addr, port, heartbeat(node()))
    Process.send_after(self(), :heartbeat, :rand.uniform(5_000))
    {:noreply, state}
  end

  # Handle received heartbeats
  def handle_info({:udp, _socket, _ip, _port, packet}, state) do
    handle_heartbeat(packet)
    {:noreply, state}
  end

  def terminate(_type, _reason, {_,_,socket}) do
    :gen_udp.close(socket)
    :ok
  end

  # Construct iodata representing packet to send
  defp heartbeat(node_name) do
    ["heartbeat::", :erlang.term_to_binary(%{node: node_name})]
  end

  # Upon receipt of a heartbeat, we check to see if the node
  # is connected to us, and if not, we connect to it.
  # If the connection fails, it's likely because the cookie
  # is different, and thus a node we can ignore
  @spec handle_heartbeat(binary) :: :ok
  defp handle_heartbeat(<<"heartbeat::", rest::binary>>) do
    case :erlang.binary_to_term(rest) do
      %{node: n} when is_atom(n) ->
        nodelist = [Node.self|Node.list(:connected)]
        cond do
          not n in nodelist ->
            case :net_kernel.connect_node(n) do
              true ->
                debug "connected to #{inspect n}"
                :ok
              reason ->
                debug "attempted to connect to node (#{inspect n}) from heartbeat, but failed with #{reason}."
                :ok
            end
          :else ->
            :ok
        end
      _ ->
        :ok
    end
  end
  defp handle_heartbeat(_packet) do
    :ok
  end
end
