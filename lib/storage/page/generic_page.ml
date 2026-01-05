type 'a t =
  { page_no : int
  ; mutable dirty : bool
  ; data : 'a
  }

let create page_no data = { page_no; dirty = false; data }
let page_no p = p.page_no
let dirty p = p.dirty
let set_dirty p = p.dirty <- true
let clear_dirty p = p.dirty <- false
