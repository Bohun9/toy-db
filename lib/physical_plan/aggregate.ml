type agg_state =
  | Count of { mutable cnt : int }
  | Sum of { mutable sum : int }
  | Avg of
      { mutable sum : int
      ; mutable cnt : int
      }
  | Min of { mutable value : Core.Value.t }
  | Max of { mutable value : Core.Value.t }

type t =
  { state : agg_state
  ; expr : Expr.t
  }

let empty agg_kind typ expr =
  let state =
    match agg_kind with
    | Core.Syntax.Count -> Count { cnt = 0 }
    | Core.Syntax.Sum -> Sum { sum = 0 }
    | Core.Syntax.Avg -> Avg { sum = 0; cnt = 0 }
    | Core.Syntax.Min -> Min { value = Core.Value.plus_infty typ }
    | Core.Syntax.Max -> Max { value = Core.Value.minus_infty typ }
  in
  { state; expr }
;;

let step { state; expr } tuple =
  let v = Expr.eval expr tuple in
  match state with
  | Count s -> s.cnt <- s.cnt + 1
  | Sum s -> s.sum <- s.sum + Core.Value.value_to_int v
  | Avg s ->
    s.sum <- s.sum + Core.Value.value_to_int v;
    s.cnt <- s.cnt + 1
  | Max s -> s.value <- Core.Value.max s.value v
  | Min s -> s.value <- Core.Value.min s.value v
;;

let finalize { state; _ } =
  match state with
  | Count { cnt } -> Core.Value.Int cnt
  | Sum { sum } -> Core.Value.Int sum
  | Avg { sum; cnt } -> Core.Value.Int (sum / cnt)
  | Min { value } -> value
  | Max { value } -> value
;;
