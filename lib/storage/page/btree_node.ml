module M = Metadata

type t =
  | InternalPage of Btree_internal_page.t
  | LeafPage of Btree_leaf_page.t

let deserialize page_no sch key_attribute data =
  match Bytes.get data 0 with
  | c when c = Btree_internal_page.internal_id ->
    Btree_internal_page.deserialize
      page_no
      (M.Table_schema.column_type sch key_attribute)
      data
    |> fun p -> InternalPage p
  | c when c = Btree_leaf_page.leaf_id ->
    Btree_leaf_page.deserialize page_no sch key_attribute data |> fun p -> LeafPage p
  | _ -> failwith "internal error"
;;

let safe = function
  | InternalPage p -> Btree_internal_page.safe p
  | LeafPage p -> Btree_leaf_page.safe p
;;
