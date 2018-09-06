(*
 * Copyright (c) 2013-2015 Thomas Gazagnaire <thomas@gazagnaire.org>
 * Copyright (c) 2015 Mounir Nasr Allah <mounir@nasrallah.co>
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 *)

open Lwt.Infix
open Irmin

module Conf = struct

  let min_size =
    Irmin.Private.Conf.key ~doc:"Minimal chunk size" "min-size"
      Irmin.Private.Conf.int 4000

  let chunk_size =
    Irmin.Private.Conf.key ~doc:"Size of chunk" "size"
      Irmin.Private.Conf.int 4096

end

let chunk_size = Conf.chunk_size

let err_too_small ~min size =
  Printf.ksprintf invalid_arg
    "Chunks of %d bytes are too small. Size should at least be %d bytes."
    size min

let config ?(config=Irmin.Private.Conf.empty) ?size ?min_size () =
  let module C = Irmin.Private.Conf in
  let min_size = match min_size with
    | None   -> C.default Conf.min_size
    | Some v -> v
  in
  let size = match size with
    | None   -> C.default Conf.chunk_size
    | Some v -> if v < min_size then err_too_small ~min:min_size v else v
  in
  C.add (C.add config Conf.min_size min_size) Conf.chunk_size size

module Chunk (K: Irmin.Hash.S) = struct

  type t =
    | Data     of { payload: bytes; padding   : bytes }
    | Indirect of { payload: K.t list; padding: bytes }

  let t =
    let open Irmin.Type in
    variant "chunk" (fun d i -> function
        | Data     {payload; padding} -> d (payload, padding)
        | Indirect {payload; padding} -> i (payload, padding) )
    |~ case1 "Data" (pair bytes bytes)
      (fun (payload, padding) -> Data {payload; padding})
    |~ case1 "Indirect" (pair (list K.t) bytes)
      (fun (payload, padding) -> Indirect {payload; padding})
    |> sealv

  let of_string s = Irmin.Type.decode_string t s
  let pp ppf v = Fmt.string ppf (Irmin.Type.encode_string t v)

end

module AO (S:AO_MAKER) (K:Irmin.Hash.S) (V: Irmin.Contents.Conv) = struct

  module Chunk = Chunk(K)

  module AO = S(K)(Chunk)
  type key = AO.key
  type value = V.t

  type t = {
    db          : AO.t;             (* An handler to the underlying database. *)
    chunk_size  : int;                                 (* the size of chunks. *)
    max_children: int;     (* the maximum number of children a node can have. *)
    max_data    : int; (* the maximum length (in bytes) of data stored in one
                          chunk. *)
  }

  module Tree = struct

    let leaves t root =
      let rec aux acc = function
        | Chunk.Data {payload; _}     -> Lwt.return (acc @ [payload])
        | Chunk.Indirect {payload; _} ->
          Lwt_list.fold_left_s (fun acc key ->
              AO.find t.db key >>= function
              | None   -> Lwt.return acc
              | Some v -> aux acc v
            ) acc payload
      in
      aux [] root

    let list_split n l =
      let rec aux i acc rest =
        if i >= n then List.rev acc, 0, rest
        else match rest with
          | []   -> List.rev acc, (n-i), rest
          | h::t -> aux (i+1) (h :: acc) t
        in
        aux 0 [] l

    (* Transform a list of keys (of arbitrary size) into a tree of
       width at most t.max_children *)
    let add_keys t l =
      let rec aux l r = match l with
        | [] -> Lwt.return r
        | l ->
          let n =
            if List.length l >= t.max_children
            then t.max_children
            else List.length l
          in
          let payload, d, rest = list_split n l in
          let padding = Bytes.create (K.digest_size * d) in
          AO.add t.db (Chunk.Indirect {payload; padding}) >>= fun x ->
          aux rest (x::r)
      in
      aux l []

    let add t l =
      let rec aux = function
        | []  -> invalid_arg "Irmin_chunk.Tree.add"
        | [k] -> Lwt.return k
        | l   -> add_keys t l >>= aux
      in
      aux l

  end

  let v config =
    let module C = Irmin.Private.Conf in
    let chunk_size = C.get config Conf.chunk_size in
    let max_data = chunk_size in
    let max_children = chunk_size / K.digest_size in
    if max_children <= 1 then (
      let min = 1 + K.digest_size * 2 in
      err_too_small ~min chunk_size;
    );
    AO.v config >|= fun db -> { db; chunk_size; max_children; max_data }

  let find_leaves t key =
    AO.find t.db key >>= function
    | None    -> Lwt.return []
    | Some x  -> Tree.leaves t x

  let find t key =
    find_leaves t key >|= fun bufs ->
    let buf = Bytes.concat Bytes.empty bufs in
    match Irmin.Type.decode_bytes V.t buf with
    |Ok va   -> Some va
    |Error _ -> None

  let list_range ~init ~stop ~step =
    let rec aux acc n =
      if n >= stop then List.rev acc
      else aux (n :: acc) (n + step)
    in
    aux [] init

  let add t v =
    let buf = Irmin.Type.encode_bytes V.t v in
    let len = Bytes.length buf in
    if len <= t.max_data then (
      let payload = buf in
      let padding = Bytes.create (t.max_data - len) in
      AO.add t.db (Chunk.Data {payload; padding})
    ) else (
      let offs = list_range ~init:0 ~stop:len ~step:t.max_data in
      let aux off =
        let len = min t.max_data (Bytes.length buf - off) in
        let payload = Bytes.sub buf off len in
        let padding = Bytes.create (t.max_data - len) in
        AO.add t.db (Chunk.Data {payload; padding})
      in
      Lwt_list.map_s aux offs >>= Tree.add t
    )

  let mem t key = AO.mem t.db key

end

module AO_stable
    (L: LINK_MAKER) (S: AO_MAKER) (K: Hash.S) (V: Irmin.Contents.Conv)
= struct

  module AO = AO(S)(K)(V)
  module Link = L(K)

  type key = K.t
  type value = V.t
  type t = { ao: AO.t; link: Link.t }

  let v config =
    AO.v config >>= fun ao ->
    Link.v config >|= fun link ->
      { ao; link; }

  let find t k =
    Link.find t.link k >>= function
    | None   -> Lwt.return_none
    | Some k -> AO.find t.ao k

  let mem t k =
    Link.find t.link k >>= function
    | None   -> Lwt.return_false
    | Some k -> AO.mem t.ao k

  let add t v =
    AO.add t.ao v >>= fun k' ->
    let k = K.digest V.t v in
    Link.add t.link k k' >>= fun () ->
    Lwt.return k

end
