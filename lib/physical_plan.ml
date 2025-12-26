type expr =
  | EValue of Value.t
  | EAttributeFetch of int

let eval_expr e t =
  match e with
  | EValue v -> v
  | EAttributeFetch i -> Tuple.field t i
;;

type physical_plan_data =
  | SeqScan of { file : Table_registry.packed_dbfile }
  | Insert of
      { child : t
      ; file : Table_registry.packed_dbfile
      }
  | Const of { tuples : Tuple.t list }
  | Filter of
      { child : t
      ; e1 : expr
      ; op : Syntax.relop
      ; e2 : expr
      }
  | Join of
      { child1 : t
      ; child2 : t
      ; e1 : expr
      ; e2 : expr
      }

and t =
  { desc : Tuple_desc.t
  ; plan : physical_plan_data
  }

let make_pp desc plan = { desc; plan }

let rec build_plan_table_expr reg = function
  | Logical_plan.Table { name; alias } ->
    let file = Table_registry.get_table reg name in
    let (Table_registry.PackedDBFile (m, f)) = file in
    let module M = (val m) in
    make_pp (Tuple_desc.from_table_schema (M.schema f) alias) @@ SeqScan { file }
  | Logical_plan.Join { tab1; tab2; field1; field2 } ->
    let pp1 = build_plan_table_expr reg tab1 in
    let pp2 = build_plan_table_expr reg tab2 in
    let field_index1 = Tuple_desc.field_index pp1.desc field1 in
    let field_index2 = Tuple_desc.field_index pp2.desc field2 in
    make_pp (Tuple_desc.combine pp1.desc pp2.desc)
    @@ Join
         { child1 = pp1
         ; child2 = pp2
         ; e1 = EAttributeFetch field_index1
         ; e2 = EAttributeFetch field_index2
         }
;;

let build_plan_predicate pp alias ({ column; op; value } : Logical_plan.predicate) =
  let field_index = Tuple_desc.field_index pp.desc { table_alias = alias; column } in
  make_pp pp.desc
  @@ Filter
       { child = pp
       ; e1 = EAttributeFetch field_index
       ; op
       ; e2 = EValue (Value.trans value)
       }
;;

let build_plan_predicates pp (alias, preds) =
  List.fold_left (fun acc pred -> build_plan_predicate pp alias pred) pp preds
;;

let build_plan reg logical_plan =
  match logical_plan with
  | Logical_plan.Select { table_expr; predicates } ->
    let pp = build_plan_table_expr reg table_expr in
    let predicates = predicates |> Hashtbl.to_seq |> List.of_seq in
    List.fold_left build_plan_predicates pp predicates
  | Logical_plan.InsertValues { table; tuples } ->
    let file = Table_registry.get_table reg table in
    make_pp
      Tuple_desc.dummy
      (Insert
         { child =
             make_pp
               Tuple_desc.dummy
               (Const { tuples = List.map Tuple.trans_tuple tuples })
         ; file
         })
;;

let rec execute_plan' tid pp =
  match pp with
  | SeqScan { file } ->
    let (Table_registry.PackedDBFile (m, f)) = file in
    let module M = (val m) in
    M.scan_file f tid
  | Insert { child; file } ->
    let (Table_registry.PackedDBFile (m, f)) = file in
    let module M = (val m) in
    Seq.iter (fun t -> M.insert_tuple f t tid) (execute_plan tid child);
    Seq.empty
  | Const { tuples } -> List.to_seq tuples
  | Filter { child; e1; op; e2 } ->
    Seq.filter
      (fun t ->
         let v1 = eval_expr e1 t in
         let v2 = eval_expr e2 t in
         Value.value_eval_relop v1 op v2)
      (execute_plan tid child)
  | Join { child1; child2; e1; e2 } ->
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

and execute_plan tid { plan; _ } = execute_plan' tid plan
