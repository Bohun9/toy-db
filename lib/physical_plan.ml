type expr =
  | EValue of Tuple.value
  | EField of Syntax.field_name
  | EBinop of expr * Syntax.binop * expr

let rec trans_expr (e : Syntax.expr) =
  match e with
  | Syntax.EValue v -> EValue (Tuple.trans_value v)
  | Syntax.EField fn -> EField fn
  | Syntax.EBinop (e1, op, e2) -> EBinop (trans_expr e1, op, trans_expr e2)
;;

let get_field_index desc fn =
  match List.find_index (fun (Tuple.FieldMetadata { name; _ }) -> name = fn) desc with
  | Some i -> i
  | None -> failwith "internal error - get_field_index"
;;

let rec eval_expr e (t : Tuple.t) =
  match e with
  | EValue v -> v
  | EField fn -> List.nth t.values (get_field_index t.desc fn)
  | EBinop (e1, op, e2) ->
    let v1 = eval_expr e1 t in
    let v2 = eval_expr e2 t in
    (match op with
     | Eq -> Tuple.VBool (v1 = v2)
     | Neq -> Tuple.VBool (v1 <> v2)
     | Add ->
       (match v1, v2 with
        | Tuple.VInt n1, Tuple.VInt n2 -> Tuple.VInt (n1 + n2)
        | Tuple.VString s1, Tuple.VString s2 -> Tuple.VString (s1 ^ s2)
        | _ -> raise (Error.DBError Error.Type_mismatch)))
;;

type t =
  | SeqScan of
      { file : Table_registry.packed_dbfile
      ; alias : string
      }
  | Insert of
      { file : Table_registry.packed_dbfile
      ; child : t
      }
  | Const of { tuples : Tuple.t list }
  | Filter of
      { pred : expr
      ; child : t
      }
  | Join of
      { e1 : expr
      ; e2 : expr
      ; child1 : t
      ; child2 : t
      }

let rec make_plan_table_expr cat = function
  | Syntax.Table { name; alias } ->
    let alias = Option.value alias ~default:name in
    SeqScan { file = Table_registry.get_table cat name; alias }
  | Syntax.Join { tab1; tab2; e1; e2 } ->
    Join
      { e1 = trans_expr e1
      ; e2 = trans_expr e2
      ; child1 = make_plan_table_expr cat tab1
      ; child2 = make_plan_table_expr cat tab2
      }
;;

let make_plan_predicates fs pp =
  List.fold_left (fun acc f -> Filter { pred = trans_expr f; child = acc }) pp fs
;;

let make_plan cat stmt =
  match stmt with
  | Syntax.Select { exprs; table_expr; predicates } ->
    make_plan_table_expr cat table_expr |> make_plan_predicates predicates
  | Syntax.InsertValues { table; tuples } ->
    Insert
      { file = Table_registry.get_table cat table
      ; child = Const { tuples = List.map Tuple.trans_tuple tuples }
      }
;;

let rec execute_plan tid pp =
  match pp with
  | SeqScan { file; alias } ->
    let (Table_registry.PackedDBFile (m, f)) = file in
    let module M = (val m) in
    Seq.map (Tuple.set_tuple_alias alias) (M.scan_file f tid)
  | Insert { file; child } ->
    let (Table_registry.PackedDBFile (m, f)) = file in
    let module M = (val m) in
    Seq.iter (fun t -> M.insert_tuple f t tid) (execute_plan tid child);
    Seq.empty
  | Const { tuples } -> List.to_seq tuples
  | Filter { pred; child } ->
    Seq.filter (fun t -> eval_expr pred t = Tuple.VBool true) (execute_plan tid child)
  | Join { e1; e2; child1; child2 } ->
    fun () ->
      let groups = Hashtbl.create 16 in
      Seq.iter
        (fun t1 ->
          let v1 = eval_expr e1 t1 in
          let new_group =
            match Hashtbl.find_opt groups v1 with
            | Some group -> t1 :: group
            | None -> [ t1 ]
          in
          Hashtbl.replace groups v1 new_group)
        (execute_plan tid child1);
      Seq.flat_map
        (fun t2 ->
          let v2 = eval_expr e2 t2 in
          match Hashtbl.find_opt groups v2 with
          | Some group -> List.to_seq (List.map (Fun.flip Tuple.combine_tuple t2) group)
          | None -> Seq.empty)
        (execute_plan tid child2)
        ()
;;
