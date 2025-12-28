type bound =
  | Inclusive
  | Exclusive

type endpoint =
  { value : Value.t
  ; bound : bound
  }

let minus_infty_endpoint t = { value = Value.minus_infty t; bound = Inclusive }
let plus_infty_endpoint t = { value = Value.plus_infty t; bound = Inclusive }

type t =
  { lower : endpoint
  ; upper : endpoint
  }

let unbounded t = { lower = minus_infty_endpoint t; upper = plus_infty_endpoint t }

let interval_of_predicate op v =
  let t = Value.derive_type v in
  match op with
  | Syntax.Eq ->
    { lower = { value = v; bound = Inclusive }; upper = { value = v; bound = Inclusive } }
  | Syntax.Le ->
    { lower = minus_infty_endpoint t; upper = { value = v; bound = Inclusive } }
  | Syntax.Lt ->
    { lower = minus_infty_endpoint t; upper = { value = v; bound = Exclusive } }
  | Syntax.Ge ->
    { lower = { value = v; bound = Inclusive }; upper = plus_infty_endpoint t }
  | Syntax.Gt ->
    { lower = { value = v; bound = Exclusive }; upper = plus_infty_endpoint t }
;;

let max_lower e1 e2 =
  match Value.compare e1.value e2.value with
  | x when x > 0 -> e1
  | x when x < 0 -> e2
  | _ -> { e1 with bound = max e1.bound e2.bound }
;;

let min_upper e1 e2 =
  match Value.compare e1.value e2.value with
  | x when x > 0 -> e2
  | x when x < 0 -> e1
  | _ -> { e1 with bound = max e1.bound e2.bound }
;;

let intersect i1 i2 =
  { lower = max_lower i1.lower i2.lower; upper = min_upper i1.upper i2.upper }
;;

let constrain interval op v = intersect interval (interval_of_predicate op v)
let left_endpoint interval = interval.lower.value

let satisfies_lower_bound interval v =
  let endpoint = interval.lower in
  let op =
    match endpoint.bound with
    | Inclusive -> Syntax.Le
    | Exclusive -> Syntax.Lt
  in
  Value.eval_relop endpoint.value op v
;;

let satisfies_upper_bound interval v =
  let endpoint = interval.upper in
  let op =
    match endpoint.bound with
    | Inclusive -> Syntax.Le
    | Exclusive -> Syntax.Lt
  in
  Value.eval_relop v op endpoint.value
;;

let inside interval v =
  satisfies_lower_bound interval v && satisfies_upper_bound interval v
;;
