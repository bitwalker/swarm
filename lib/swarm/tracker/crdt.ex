defmodule Swarm.IntervalTreeClock do
  @moduledoc """
  This is an implementation of an Interval Clock Tree, ported from
  the implementation in Erlang written by Paulo Sergio Almeida <psa@di.uminho.pt>
  found [here](https://github.com/ricardobcl/Interval-Tree-Clocks/blob/master/erlang/itc.erl).
  """
  use Bitwise
  import Kernel, except: [max: 2, min: 2]
  @compile {:inline, [min: 2, max: 2,
                      drop: 2, lift: 2,
                      base: 1, height: 1]}

  @type int_tuple :: {non_neg_integer, non_neg_integer}
  @type t :: int_tuple |
             {int_tuple, non_neg_integer} |
             {non_neg_integer, int_tuple} |
             {int_tuple, int_tuple}

  @doc """
  Creates a new interval tree clock
  """
  @spec seed() :: __MODULE__.t
  def seed(), do: {1, 0}

  @doc """
  Joins two forked clocks into a single clock with both causal histories,
  used for retiring a replica.
  """
  @spec join(__MODULE__.t, __MODULE__.t) :: __MODULE__.t
  def join({i1, e1}, {i2, e2}), do: {sum(i1, i2), join_ev(e1, e2)}

  @doc """
  Forks a clock containing a shared causal history, used for creating new replicas.
  """
  @spec fork(__MODULE__.t) :: __MODULE__.t
  def fork({i, e}) do
    {i1, i2} = split(i)
    {{i1, e}, {i2, e}}
  end

  @doc """
  Gets a snapshot of a clock without its identity. Useful for sending the clock with messages,
  but cannot be used to track events.
  """
  @spec peek(__MODULE__.t) :: __MODULE__.t
  def peek({i, e}), do: {{0, e}, {i, e}}

  @doc """
  Records an event on the given clock
  """
  @spec event(__MODULE__.t) :: __MODULE__.t
  def event({i, e}) do
    case fill(i, e) do
      ^e ->
        {_, e1} = grow(i, e)
        {i, e1}
      e1 ->
        {i, e1}
    end
  end

  @doc """
  Determines if the left-hand clock is causally dominated by the right-hand clock.
  If the left-hand clock is LEQ than the right-hand clock, and vice-versa, then they are
  causally equivalent.
  """
  @spec leq(__MODULE__.t, __MODULE__.t) :: boolean
  def leq({_, e1}, {_, e2}), do: leq_ev(e1, e2)

  @doc """
  Compares two clocks.
  If :eq is returned, the two clocks are causally equivalent
  If :lt is returned, the first clock is causally dominated by the second
  If :gt is returned, the second clock is causally dominated by the first
  If :concurrent is returned, the two clocks are concurrent (conflicting)
  """
  @spec compare(__MODULE__.t, __MODULE__.t) :: :lt | :gt | :eq | :concurrent
  def compare(a, b) do
    a_leq = leq(a, b)
    b_leq = leq(b, a)
    cond do
      a_leq and b_leq -> :eq
      a_leq           -> :lt
      b_leq           -> :gt
      :else           -> :concurrent
    end
  end

  @doc """
  Encodes the clock as a binary
  """
  @spec encode(__MODULE__.t) :: binary
  def encode({i, e}), do: :erlang.term_to_binary({i, e})

  @doc """
  Decodes the clock from a binary
  """
  @spec decode(binary) :: {:ok, __MODULE__.t} | {:error, {:invalid_clock, term}}
  def decode(b) when is_binary(b) do
    case :erlang.binary_to_term(b) do
      {_i, _e} = clock ->
        clock
      other ->
        {:error, {:invalid_clock, other}}
    end
  end

  @doc """
  Returns the length of the encoded binary representation of the clock
  """
  @spec len(__MODULE__.t) :: non_neg_integer
  def len(d), do: :erlang.size(encode(d))

  ## Private API

  defp leq_ev({n1, l1, r1}, {n2, l2, r2}) do
    n1 <= n2 and
    leq_ev(lift(n1, l1), lift(n2, l2)) and
    leq_ev(lift(n1, r1), lift(n2, r2))
  end
  defp leq_ev({n1, l1, r1}, n2) do
    n1 <= n2 and
    leq_ev(lift(n1, l1), n2) and
    leq_ev(lift(n1, r1), n2)
  end
  defp leq_ev(n1, {n2, _, _}), do: n1 <= n2
  defp leq_ev(n1, n2), do: n1 <= n2

  defp norm_id({0, 0}), do: 0
  defp norm_id({1, 1}), do: 1
  defp norm_id(x),      do: x

  defp norm_ev({n, m, m}) when is_integer(m), do: n + m
  defp norm_ev({n, l, r}) do
    m = min(base(l), base(r))
    {n+m, drop(m, l), drop(m, r)}
  end

  defp sum(0, x), do: x
  defp sum(x, 0), do: x
  defp sum({l1, r1}, {l2, r2}), do: norm_id({sum(l1, l2), sum(r1, r2)})

  defp split(0), do: {0, 0}
  defp split(1), do: {{1, 0}, {0, 1}}
  defp split({0, i}) do
    {i1, i2} = split(i)
    {{0, i1}, {0, i2}}
  end
  defp split({i, 0}) do
    {i1, i2} = split(i)
    {{i1, 0}, {i2, 0}}
  end
  defp split({i1, i2}), do: {{i1,0}, {0,i2}}

  defp join_ev({n1, _, _} = e1, {n2, _, _} = e2) when n1 > n2, do: join_ev(e2, e1)
  defp join_ev({n1, l1, r1}, {n2, l2, r2}) when n1 <= n2 do
    d = n2 - n1
    norm_ev({n1, join_ev(l1, lift(d, l2)), join_ev(r1, lift(d, r2))})
  end
  defp join_ev(n1, {n2, l2, r2}), do: join_ev({n1,0,0}, {n2,l2,r2})
  defp join_ev({n1, l1, r1}, n2), do: join_ev({n1,l1,r1}, {n2,0,0})
  defp join_ev(n1, n2), do: max(n1, n2)

  defp fill(0, e), do: e
  defp fill(1, {_,_,_}=e), do: height(e)
  defp fill(_, n) when is_integer(n), do: n
  defp fill({1, r}, {n, el, er}) do
    er1 = fill(r, er)
    d = max(height(el), base(er1))
    norm_ev({n, d, er1})
  end
  defp fill({l, 1}, {n, el, er}) do
    el1 = fill(l, el)
    d = max(height(er), base(el1))
    norm_ev({n, el1, d})
  end
  defp fill({l, r}, {n, el, er}) do
    norm_ev({n, fill(l, el), fill(r, er)})
  end

  defp grow(1, n) when is_integer(n), do: {0, n+1}
  defp grow({0, i}, {n, l, r}) do
    {h, e1} = grow(i, r)
    {h+1, {n, l, e1}}
  end
  defp grow({i, 0}, {n, l, r}) do
    {h, e1} = grow(i, l)
    {h+1, {n, e1, r}}
  end
  defp grow({il, ir}, {n, l, r}) do
    {hl, el} = grow(il, l)
    {hr, er} = grow(ir, r)
    cond do
      hl < hr -> {hl+1, {n, el, r}}
      :else   -> {hr+1, {n, l, er}}
    end
  end
  defp grow(i, n) when is_integer(n) do
    {h, e} = grow(i, {n, 0, 0})
    {h+100_000, e}
  end

  defp height({n, l, r}), do: n + max(height(l), height(r))
  defp height(n), do: n

  defp base({n, _, _}), do: n
  defp base(n), do: n

  defp lift(m, {n, l, r}), do: {n+m, l, r}
  defp lift(m, n), do: n + m

  defp drop(m, {n, l, r}) when m <= n, do: {n-m, l, r}
  defp drop(m, n) when m <= n, do: n - m

  defp max(x, y) when x <= y, do: y
  defp max(x, _), do: x

  defp min(x, y) when x <= y, do: x
  defp min(_, y), do: y

  def str({i, e}), do: List.to_string(List.flatten([List.flatten(stri(i)), List.flatten(stre(e))]))

  defp stri(0), do: '0'
  defp stri(1), do: ''
  defp stri({0, i}), do: 'R'++stri(i)
  defp stri({i, 0}), do: 'L'++stri(i)
  defp stri({l, r}), do: ['(L'++stri(l), '+', 'R'++stri(r), ')']

  defp stre({n, l, 0}), do: [stre(n), 'L', stre(l)]
  defp stre({n, 0, r}), do: [stre(n), 'R', stre(r)]
  defp stre({n, l, r}), do: [stre(n), '(L', stre(l), '+R', stre(r), ')']
  defp stre(n) when n > 0, do: :erlang.integer_to_list(n)
  defp stre(_), do: ''

end
