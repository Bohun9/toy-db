type t =
  { data : bytes
  ; mutable pos : int
  }

let create data = { data; pos = 0 }

let read_int16_le c =
  let n = Bytes.get_int16_le c.data c.pos in
  c.pos <- c.pos + 2;
  n
;;

let read_int64_le c =
  let n = Bytes.get_int64_le c.data c.pos in
  c.pos <- c.pos + 8;
  n
;;

let read_char c =
  let ch = Bytes.get c.data c.pos in
  c.pos <- c.pos + 1;
  ch
;;

let read_string c n =
  let b = Bytes.sub c.data c.pos n in
  c.pos <- c.pos + n;
  String.of_bytes b
;;
