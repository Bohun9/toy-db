open Core
open Metadata

type grouper_output_item =
  | GrouperAttribute of { group_by_index : int }
  | GrouperAggregate

type order_item =
  { order_by_expr : Expr.t
  ; order : Syntax.order
  }

type physical_plan_data =
  | SeqScan of { file : Db_file.table_file }
  | RangeScan of
      { file : Db_file.index_file
      ; interval : Value_interval.t
      }
  | Const of { tuples : Tuple.t list }
  | Insert of
      { child : t
      ; file : Db_file.table_file
      }
  | Project of
      { child : t
      ; exprs : Expr.t list
      }
  | Filter of
      { child : t
      ; e1 : Expr.t
      ; op : Syntax.relop
      ; e2 : Expr.t
      }
  | Join of
      { child1 : t
      ; child2 : t
      ; e1 : Expr.t
      ; e2 : Expr.t
      }
  | Grouper of
      { child : t
      ; group_by_exprs : Expr.t list
      ; create_aggregates : unit -> Aggregate.t list
      ; output : grouper_output_item list
      }
  | Sorter of
      { child : t
      ; order : order_item list
      }
  | Limiter of
      { child : t
      ; limit : int option
      ; offset : int
      }

and t =
  { desc : Tuple_desc.t
  ; plan : physical_plan_data
  }

let make_pp desc plan = { desc; plan }
let fields_to_exprs fs desc = List.map (fun f -> Expr.of_table_field f desc) fs

let build_plan_predicate pp ({ field; op; value } : Logical_plan.predicate) =
  make_pp pp.desc
  @@ Filter
       { child = pp
       ; e1 = Expr.of_table_field field pp.desc
       ; op
       ; e2 = EValue (Value.trans value)
       }
;;

let build_plan_predicates pp preds =
  List.fold_left (fun acc pred -> build_plan_predicate pp pred) pp preds
;;

let index_interval_from_predicates predicates key_type =
  List.fold_left
    (fun acc ({ op; value; _ } : Logical_plan.predicate) ->
       Value_interval.constrain acc op (Value.trans value))
    (Value_interval.unbounded key_type)
    predicates
;;

let rec build_plan_table_expr reg grouped_predicates = function
  | Logical_plan.Table { name; alias } ->
    let predicates = Hashtbl.find grouped_predicates alias in
    (match Table_registry.get_table reg name with
     | Db_file.TableFile file ->
       let (Db_file.PackedTable (m, f)) = file in
       let module M = (val m) in
       let pp =
         make_pp (Tuple_desc.from_table_schema (M.schema f) alias) @@ SeqScan { file }
       in
       build_plan_predicates pp predicates
     | Db_file.IndexFile file ->
       let (Db_file.PackedIndex (m, f)) = file in
       let module M = (val m) in
       let index_key = M.key_info f in
       let index_predicates, other_predicates =
         List.partition
           (fun ({ field; _ } : Logical_plan.predicate) -> index_key.name = field.column)
           predicates
       in
       let index_interval =
         index_interval_from_predicates index_predicates index_key.typ
       in
       let pp =
         make_pp (Tuple_desc.from_table_schema (M.schema f) alias)
         @@ RangeScan { file; interval = index_interval }
       in
       build_plan_predicates pp other_predicates)
  | Logical_plan.Join { tab1; tab2; field1; field2 } ->
    let child1 = build_plan_table_expr reg grouped_predicates tab1 in
    let child2 = build_plan_table_expr reg grouped_predicates tab2 in
    make_pp (Tuple_desc.combine child1.desc child2.desc)
    @@ Join
         { child1
         ; child2
         ; e1 = Expr.of_table_field field1 child1.desc
         ; e2 = Expr.of_table_field field2 child2.desc
         }
;;

let build_plan reg logical_plan =
  match logical_plan with
  | Logical_plan.Select { table_expr; predicates; grouping; order; limit; offset } ->
    let table_expr_pp = build_plan_table_expr reg predicates table_expr in
    let grouping_pp =
      match grouping with
      | Logical_plan.NoGrouping { select_list = Logical_plan.Star } -> table_expr_pp
      | Logical_plan.NoGrouping { select_list = Logical_plan.SelectFields select_fields }
        ->
        make_pp (Tuple_desc.from_table_fields select_fields)
        @@ Project
             { child = table_expr_pp
             ; exprs = fields_to_exprs select_fields table_expr_pp.desc
             }
      | Logical_plan.Grouping { select_list; group_by_fields } ->
        let group_by_exprs = fields_to_exprs group_by_fields table_expr_pp.desc in
        let create_aggregates () =
          List.filter_map
            (fun select_item ->
               match select_item with
               | Logical_plan.SelectAggregate { agg_kind; field; _ } ->
                 Some
                   (Aggregate.empty
                      agg_kind
                      field.typ
                      (Expr.of_table_field field table_expr_pp.desc))
               | Logical_plan.SelectField _ -> None)
            select_list
        in
        let output =
          List.map
            (fun select_item ->
               match select_item with
               | Logical_plan.SelectField { group_by_index; _ } ->
                 GrouperAttribute { group_by_index }
               | Logical_plan.SelectAggregate _ -> GrouperAggregate)
            select_list
        in
        make_pp (Tuple_desc.from_grouping select_list)
        @@ Grouper { child = table_expr_pp; group_by_exprs; create_aggregates; output }
    in
    let order_pp =
      match order with
      | Some order_list ->
        make_pp grouping_pp.desc
        @@ Sorter
             { child = grouping_pp
             ; order =
                 List.map
                   (fun ({ field; order } : Logical_plan.order_item) ->
                      { order_by_expr = Expr.of_field field grouping_pp.desc; order })
                   order_list
             }
      | None -> grouping_pp
    in
    let limit_pp =
      match limit, offset with
      | None, None -> order_pp
      | _ ->
        make_pp order_pp.desc
        @@ Limiter { child = order_pp; limit; offset = Option.value offset ~default:0 }
    in
    limit_pp
  | Logical_plan.InsertValues { table; tuples } ->
    let file = Table_registry.get_table reg table in
    make_pp
      Tuple_desc.dummy
      (Insert
         { child =
             make_pp
               Tuple_desc.dummy
               (Const { tuples = List.map Tuple.trans_tuple tuples })
         ; file = Db_file.to_table_file file
         })
;;

let eval_exprs es t = List.map (fun e -> Expr.eval e t) es

let rec execute_plan' tid pp =
  match pp with
  | SeqScan { file = Db_file.PackedTable (m, f) } ->
    let module M = (val m) in
    M.scan_file f tid
  | RangeScan { file = Db_file.PackedIndex (m, f); interval } ->
    let module M = (val m) in
    M.range_scan f interval tid
  | Const { tuples } -> List.to_seq tuples
  | Insert { child; file = Db_file.PackedTable (m, f) } ->
    let module M = (val m) in
    Seq.iter (fun t -> M.insert_tuple f t tid) (execute_plan tid child);
    Seq.empty
  | Project { child; exprs } ->
    Seq.map
      (fun t -> Core.Tuple.{ values = eval_exprs exprs t; rid = None })
      (execute_plan tid child)
  | Filter { child; e1; op; e2 } ->
    Seq.filter
      (fun t ->
         let v1 = Expr.eval e1 t in
         let v2 = Expr.eval e2 t in
         Value.eval_relop v1 op v2)
      (execute_plan tid child)
  | Join { child1; child2; e1; e2 } ->
    fun () ->
      let groups = Hashtbl.create 16 in
      Seq.iter
        (fun t1 ->
           let v1 = Expr.eval e1 t1 in
           let new_group =
             match Hashtbl.find_opt groups v1 with
             | Some group -> t1 :: group
             | None -> [ t1 ]
           in
           Hashtbl.replace groups v1 new_group)
        (execute_plan tid child1);
      Seq.flat_map
        (fun t2 ->
           let v2 = Expr.eval e2 t2 in
           match Hashtbl.find_opt groups v2 with
           | Some group -> List.to_seq (List.map (Fun.flip Tuple.combine_tuple t2) group)
           | None -> Seq.empty)
        (execute_plan tid child2)
        ()
  | Grouper { child; group_by_exprs; create_aggregates; output } ->
    let group_aggregates = Hashtbl.create 16 in
    Seq.iter
      (fun tuple ->
         let group = eval_exprs group_by_exprs tuple in
         let aggregates =
           match Hashtbl.find_opt group_aggregates group with
           | Some aggs -> aggs
           | None ->
             let aggs = create_aggregates () in
             Hashtbl.add group_aggregates group aggs;
             aggs
         in
         List.iter (fun agg -> Aggregate.step agg tuple) aggregates)
      (execute_plan tid child);
    group_aggregates
    |> Hashtbl.to_seq
    |> Seq.map (fun (group, aggregates) ->
      let agg_index = ref 0 in
      let values =
        List.map
          (fun output_item ->
             match output_item with
             | GrouperAttribute { group_by_index } -> List.nth group group_by_index
             | GrouperAggregate ->
               let agg = List.nth aggregates !agg_index in
               incr agg_index;
               Aggregate.finalize agg)
          output
      in
      Core.Tuple.{ values; rid = None })
  | Sorter { child; order } ->
    let order_by_exprs = List.map (fun { order_by_expr; _ } -> order_by_expr) order in
    let order_specifiers = List.map (fun { order; _ } -> order) order in
    let tuples =
      execute_plan tid child
      |> Seq.map (fun t -> eval_exprs order_by_exprs t, t)
      |> List.of_seq
    in
    let compare (keys1, _) (keys2, _) =
      let combined = List.combine (List.combine keys1 keys2) order_specifiers in
      let rec cmp = function
        | [] -> 0
        | ((k1, k2), ord) :: rest ->
          let op =
            match ord with
            | Syntax.Asc -> Value.compare
            | Syntax.Desc -> Fun.flip Value.compare
          in
          let c = op k1 k2 in
          if c = 0 then cmp rest else c
      in
      cmp combined
    in
    let sorted_tuples = List.sort compare tuples in
    sorted_tuples |> List.map snd |> List.to_seq
  | Limiter { child; limit; offset } ->
    execute_plan tid child
    |> Seq.drop offset
    |> Option.value (Option.map Seq.take limit) ~default:Fun.id

and execute_plan tid { plan; _ } = execute_plan' tid plan
