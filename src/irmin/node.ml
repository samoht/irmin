(*
 * Copyright (c) 2013      Louis Gesbert     <louis.gesbert@ocamlpro.com>
 * Copyright (c) 2013-2017 Thomas Gazagnaire <thomas@gazagnaire.org>
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

let src = Logs.Src.create "irmin.node" ~doc:"Irmin trees/nodes"

module Log = (val Logs.src_log src : Logs.LOG)

module No_metadata = struct
  type t = unit

  let t = Type.unit

  let default = ()

  let merge = Merge.v t (fun ~old:_ () () -> Merge.ok ())
end

module Make
    (K : S.HASH) (P : sig
        type step

        val step_t : step Type.t
    end)
    (M : S.METADATA) =
struct
  type hash = K.t

  type step = P.step

  type metadata = M.t

  type kind = [ `Node | `Contents of M.t ]

  type entry = { kind : kind; name : P.step; node : K.t }

  let kind_t =
    let open Type in
    variant "Tree.kind" (fun node contents contents_m -> function
      | `Node -> node
      | `Contents m ->
          if Type.equal M.t m M.default then contents else contents_m m )
    |~ case0 "node" `Node
    |~ case0 "contents" (`Contents M.default)
    |~ case1 "contents" M.t (fun m -> `Contents m)
    |> sealv

  let entry_t : entry Type.t =
    let open Type in
    record "Tree.entry" (fun kind name node -> { kind; name; node })
    |+ field "kind" kind_t (function { kind; _ } -> kind)
    |+ field "name" P.step_t (fun { name; _ } -> name)
    |+ field "node" K.t (fun { node; _ } -> node)
    |> sealr

  let to_entry (k, v) =
    match v with
    | `Node h -> { name = k; kind = `Node; node = h }
    | `Contents (h, m) -> { name = k; kind = `Contents m; node = h }

  let of_entry n =
    ( n.name,
      match n.kind with
      | `Node -> `Node n.node
      | `Contents m -> `Contents (n.node, m) )

  module StepMap = Map.Make (struct
    type t = P.step

    let compare = Type.compare P.step_t
  end)

  type value = [ `Contents of hash * metadata | `Node of hash ]

  type node = Entries of entry StepMap.t | Inodes of v array

  and v = { level : int; node : node }

  type t = {
    level : int;
    entries : (step * entry) list;
    inodes : (int * hash) list
  }

  let t =
    let open Type in
    record "node" (fun level entries inodes -> { level; entries; inodes })
    |+ field "level" int (fun t -> t.level)
    |+ field "entries" (list (pair P.step_t entry_t)) (fun t -> t.entries)
    |+ field "inodes" (list (pair int K.t)) (fun t -> t.inodes)
    |> sealr

  let of_v v =
    let nodes = ref [] in
    let rec aux k (v : v) =
      match v.node with
      | Entries m ->
          k { level = v.level; entries = StepMap.bindings m; inodes = [] }
      | Inodes n ->
          let _, entries, inodes =
            Array.fold_left
              (fun (i, entries, inodes) n ->
                match n.node with
                | Entries m when StepMap.is_empty m -> (i + 1, entries, inodes)
                | Entries m when StepMap.cardinal m = 1 ->
                    let s, e = StepMap.choose m in
                    (i + 1, (s, e) :: entries, inodes)
                | _ ->
                    let n = aux k n in
                    let k = K.digest (Type.pre_digest t n) in
                    nodes := n :: !nodes;
                    (i + 1, entries, (i, k) :: inodes) )
              (0, [], []) n
          in
          k { level = v.level; entries; inodes }
    in
    let n = aux (fun x -> x) v in
    (n, !nodes)

  let max_entries = 64

  let max_inodes = 64

  let to_v t =
    assert (StepMap.cardinal t <= max_entries);
    Entries t

  let index ~level k = abs (Type.hash P.step_t ~seed:level k) mod max_inodes

  let v (l : (step * value) list) : v =
    let rec aux (k : v -> unit) level entries =
      if StepMap.cardinal entries <= max_entries then
        k { level; node = Entries entries }
      else
        let inodes =
          Array.make max_inodes { level; node = Entries StepMap.empty }
        in
        StepMap.iter
          (fun k v ->
            let i = index ~level k in
            match inodes.(i) with
            | { node = Entries m; _ } ->
                inodes.(i) <- { level; node = Entries (StepMap.add k v m) }
            | _ -> assert false )
          entries;
        Array.iteri
          (fun i -> function
            | { node = Entries m; _ } ->
                if StepMap.cardinal m <= max_entries then ()
                else aux (fun n -> inodes.(i) <- n) (level + 1) m
            | _ -> assert false )
          inodes;
        k { level; node = Inodes inodes }
    in
    let entries =
      List.fold_left
        (fun acc (k, v) -> StepMap.add k (to_entry (k, v)) acc)
        StepMap.empty l
    in
    let r = ref None in
    aux (fun k -> r := Some k) 0 entries;
    match !r with Some r -> r | None -> assert false

  let list_entries t = List.map (fun (_, e) -> of_entry e) (StepMap.bindings t)

  let list t =
    let rec aux acc t =
      match t.node with
      | Entries t -> StepMap.union (fun _ _ -> assert false) acc t
      | Inodes n -> Array.fold_left aux acc n
    in
    list_entries (aux StepMap.empty t)

  let find_entries t s =
    try
      let _, v = of_entry (StepMap.find s t) in
      Some v
    with Not_found -> None

  let find t s =
    let rec aux t =
      match t.node with
      | Entries t -> find_entries t s
      | Inodes n -> aux n.(index ~level:t.level s)
    in
    aux t

  let empty = { level = 0; node = Entries StepMap.empty }

  let is_empty t = list t = []

  let update t k x =
    (* FIXME(samoht): not optimized at all *)
    v ((k, x) :: list t)

  let remove t k =
    (* FIXME(samoht): not optimized at all *)
    v (List.remove_assoc k (list t))

  let step_t = P.step_t

  let hash_t = K.t

  let metadata_t = M.t

  let default = M.default

  let value_t =
    let open Type in
    variant "value" (fun n c -> function
      | `Node h -> n h | `Contents (h, m) -> c (h, m) )
    |~ case1 "node" K.t (fun k -> `Node k)
    |~ case1 "contents" (pair K.t M.t) (fun (h, m) -> `Contents (h, m))
    |> sealv

  let entries =
    let of_entries e =
      List.fold_left (fun acc e -> StepMap.add e.name e acc) StepMap.empty e
    in
    let to_entries e = List.map to_entry (list_entries e) in
    Type.map Type.(list entry_t) of_entries to_entries
end

module Store
    (C : S.CONTENTS_STORE)
    (P : S.PATH)
    (M : S.METADATA) (S : sig
        include S.CONTENT_ADDRESSABLE_STORE with type key = C.key

        module Key : S.HASH with type t = key

        module Val :
          S.NODE
          with type t = value
           and type hash = key
           and type metadata = M.t
           and type step = P.step
    end) =
struct
  module Contents = C
  module Key = Hash.With_digest (S.Key) (S.Val)
  module Path = P
  module Metadata = M

  type 'a t = 'a C.t * 'a S.t

  type key = S.key

  type value = S.value

  let mem (_, t) = S.mem t

  let find (_, t) = S.find t

  let add (_, t) = S.add t

  let all_contents ~find t =
    S.Val.list ~find (S.Val.to_inode t) >|= fun kvs ->
    List.fold_left
      (fun acc -> function k, `Contents c -> (k, c) :: acc | _ -> acc)
      [] kvs

  let all_succ ~find t =
    S.Val.list ~find (S.Val.to_inode t) >|= fun kvs ->
    List.fold_left
      (fun acc -> function k, `Node n -> (k, n) :: acc | _ -> acc)
      [] kvs

  let contents_t = C.Key.t

  let metadata_t = M.t

  let step_t = Path.step_t

  (* [Merge.alist] expects us to return an option. [C.merge] does
     that, but we need to consider the metadata too... *)
  let merge_contents_meta c =
    (* This gets us [C.t option, S.Val.Metadata.t]. We want [(C.t *
       S.Val.Metadata.t) option]. *)
    let explode = function
      | None -> (None, M.default)
      | Some (c, m) -> (Some c, m)
    in
    let implode = function None, _ -> None | Some c, m -> Some (c, m) in
    Merge.like
      Type.(option (pair contents_t metadata_t))
      (Merge.pair (C.merge c) M.merge)
      explode implode

  let merge_contents_meta c =
    Merge.alist step_t
      Type.(pair contents_t metadata_t)
      (fun _step -> merge_contents_meta c)

  let merge_parents merge_key =
    Merge.alist step_t S.Key.t (fun _step -> merge_key)

  let merge_value ((c, n) as t) merge_key =
    let find = S.find n in
    let explode t =
      all_contents ~find t >>= fun c -> all_succ ~find t >|= fun s -> (c, s)
    in
    let implode (contents, succ) =
      let xs = List.map (fun (s, c) -> (s, `Contents c)) contents in
      let ys = List.map (fun (s, n) -> (s, `Node n)) succ in
      let n, rest = S.Val.(of_inode @@ v (xs @ ys)) in
      Lwt_list.map_p (add t) rest >|= fun _ ->
      (* FIXME(samoht): shouldn't [n] be added too ? *)
      n
    in
    let merge = Merge.pair (merge_contents_meta c) (merge_parents merge_key) in
    Merge.like_lwt S.Val.t merge explode implode

  let rec merge t =
    let merge_key =
      Merge.v (Type.option S.Key.t) (fun ~old x y ->
          Merge.(f (merge t)) ~old x y )
    in
    let empty = S.Val.empty in
    let merge = merge_value t merge_key in
    let read = function
      | None -> Lwt.return empty
      | Some k -> ( find t k >|= function None -> empty | Some v -> v )
    in
    let add v =
      if S.Val.is_empty (S.Val.to_inode v) then Lwt.return_none
      else add t v >>= fun k -> Lwt.return (Some k)
    in
    Merge.like_lwt Type.(option S.Key.t) merge read add

  module Val = S.Val
end

module Graph (S : S.NODE_STORE) = struct
  module Path = S.Path
  module Contents = S.Contents.Key
  module Metadata = S.Metadata

  type step = Path.step

  type metadata = Metadata.t

  type contents = Contents.t

  type node = S.key

  type path = Path.t

  type 'a t = 'a S.t

  type value = [ `Contents of contents * metadata | `Node of node ]

  let empty t = S.add t S.Val.empty

  let list t n =
    Log.debug (fun f -> f "steps");
    let find = S.find t in
    find n >>= function
    | None -> Lwt.return []
    | Some n -> S.Val.list ~find (S.Val.to_inode n)

  module U = struct
    type t = unit

    let t = Type.unit
  end

  module Graph = Object_graph.Make (Contents) (Metadata) (S.Key) (U) (U)

  let edges db t =
    let find = S.find db in
    S.Val.list ~find (S.Val.to_inode t) >|= fun children ->
    List.fold_left
      (fun acc -> function _, `Node n -> `Node n :: acc
        | _, `Contents c -> `Contents c :: acc )
      [] children

  let pp_key = Type.pp S.Key.t

  let pp_keys = Fmt.(Dump.list pp_key)

  let pp_path = Type.pp S.Path.t

  let closure t ~min ~max =
    Log.debug (fun f -> f "closure min=%a max=%a" pp_keys min pp_keys max);
    let pred = function
      | `Node k -> (
          S.find t k >>= function None -> Lwt.return [] | Some v -> edges t v )
      | _ -> Lwt.return []
    in
    let min = List.map (fun x -> `Node x) min in
    let max = List.map (fun x -> `Node x) max in
    Graph.closure ~pred ~min ~max () >>= fun g ->
    let keys =
      List.fold_left
        (fun acc -> function `Node x -> x :: acc | _ -> acc)
        [] (Graph.vertex g)
    in
    Lwt.return keys

  let v t xs =
    let n, rest = S.Val.(of_inode @@ v xs) in
    Lwt_list.map_p (S.add t) rest >>= fun _ -> S.add t n

  let find_step t node step =
    Log.debug (fun f -> f "contents %a" pp_key node);
    let find = S.find t in
    find node >>= function
    | None -> Lwt.return None
    | Some n -> S.Val.find ~find (S.Val.to_inode n) step

  let find t node path =
    Log.debug (fun f -> f "read_node_exn %a %a" pp_key node pp_path path);
    let rec aux node path =
      match Path.decons path with
      | None -> Lwt.return (Some (`Node node))
      | Some (h, tl) -> (
          find_step t node h >>= function
          | (None | Some (`Contents _)) as x -> Lwt.return x
          | Some (`Node node) -> aux node tl )
    in
    aux node path

  let err_empty_path () = invalid_arg "Irmin.node: empty path"

  let map_one t node f label =
    Log.debug (fun f -> f "map_one %a" Type.(pp Path.step_t) label);
    let find = S.find t in
    S.Val.find ~find node label >>= fun old_key ->
    ( match old_key with
    | None | Some (`Contents _) -> Lwt.return S.Val.empty
    | Some (`Node k) -> (
        S.find t k >|= function None -> S.Val.empty | Some v -> v ) )
    >>= fun old_node ->
    let old_node = S.Val.to_inode old_node in
    f old_node >>= fun new_node ->
    if old_node == new_node then Lwt.return node
    else if S.Val.is_empty new_node then
      S.Val.remove ~find node label >|= fun node ->
      if S.Val.is_empty node then S.Val.(to_inode empty) else node
    else
      let root, rest = S.Val.of_inode new_node in
      Lwt_list.map_p (S.add t) rest >>= fun _ ->
      S.add t root >>= fun k -> S.Val.update ~find node label (`Node k)

  let map t node path f =
    Log.debug (fun f -> f "map %a %a" pp_key node pp_path path);
    let rec aux node path =
      match Path.decons path with
      | None -> f node
      | Some (h, tl) -> map_one t node (fun node -> aux node tl) h
    in
    (S.find t node >|= function None -> S.Val.empty | Some n -> n)
    >>= fun node ->
    let old_node = S.Val.to_inode node in
    aux old_node path >>= fun new_node ->
    if old_node == new_node then Lwt.return (S.Key.digest node)
    else
      let root, rest = S.Val.of_inode new_node in
      Lwt_list.map_p (S.add t) rest >>= fun _ -> S.add t root

  let update t node path n =
    Log.debug (fun f -> f "update %a %a" pp_key node pp_path path);
    let find = S.find t in
    match Path.rdecons path with
    | Some (path, file) ->
        map t node path (fun node -> S.Val.update ~find node file n)
    | None -> (
      match n with
      | `Node n -> Lwt.return n
      | `Contents _ -> failwith "TODO: Node.update" )

  let rdecons_exn path =
    match Path.rdecons path with
    | Some (l, t) -> (l, t)
    | None -> err_empty_path ()

  let remove t node path =
    let find = S.find t in
    let path, file = rdecons_exn path in
    map t node path (fun node -> S.Val.remove ~find node file)

  let path_t = Path.t

  let node_t = S.Key.t

  let metadata_t = Metadata.t

  let step_t = Path.step_t

  let contents_t = Contents.t

  let value_t =
    let open Type in
    variant "value" (fun n c -> function
      | `Node h -> n h | `Contents (h, m) -> c (h, m) )
    |~ case1 "node" node_t (fun k -> `Node k)
    |~ case1 "contents" (pair contents_t metadata_t) (fun (h, m) ->
           `Contents (h, m) )
    |> sealv
end

module V1 (N : S.NODE) = struct
  module K = struct
    let h = Type.string_of `Int64

    let size_of ?headers x =
      Type.size_of ?headers h (Type.to_bin_string N.hash_t x)

    let encode_bin ?headers buf e =
      Type.encode_bin ?headers h buf (Type.to_bin_string N.hash_t e)

    let decode_bin ?headers buf off =
      let n, v = Type.decode_bin ?headers h buf off in
      ( n,
        match Type.of_bin_string N.hash_t v with
        | Ok v -> v
        | Error (`Msg e) -> Fmt.failwith "decode_bin: %s" e )

    let t = Type.like N.hash_t ~bin:(encode_bin, decode_bin, size_of)
  end

  type step = N.step

  type hash = N.hash

  type metadata = N.metadata

  type value = N.value

  let hash_t = N.hash_t

  let metadata_t = N.metadata_t

  type 'a with_entries = { v : 'a; entries : (step * value) list }

  type inode = N.inode with_entries

  type t = N.t with_entries

  let of_tree v =
    let x, y = N.of_inode v.v in
    ({ v = x; entries = v.entries }, List.map (fun v -> { v; entries = [] }) y)

  let to_tree t =
    let v = N.to_inode t.v in
    { v; entries = t.entries }

  let inode t = t

  let internals t = (t, [])

  let import ~find v =
    N.list ~find (N.to_inode v) >|= fun entries -> { v; entries }

  let export t = t.v

  let tree entries =
    let v = N.v entries in
    { v; entries }

  let list ~find:_ t = t.entries

  let is_empty t = t.entries = []

  let default = N.default

  let find ~find t k = N.find ~find t.v k

  let update ~find t k v =
    N.update t.v ~find k v >>= fun v ->
    if t.v == v then Lwt.return t
    else N.list ~find v >|= fun entries -> { v; entries }

  let remove t ~find k =
    N.remove t.v ~find k >>= fun v ->
    if t.v == v then Lwt.return t
    else N.list ~find v >|= fun entries -> { v; entries }

  let step_t : step Type.t =
    let to_string p = Type.to_bin_string N.step_t p in
    let of_string s =
      Type.of_bin_string N.step_t s |> function
      | Ok x -> x
      | Error (`Msg e) -> Fmt.failwith "Step.of_string: %s" e
    in
    Type.(map (string_of `Int64)) of_string to_string

  let value_t =
    let open Type in
    record "node" (fun contents metadata node ->
        match (contents, metadata, node) with
        | Some c, None, None -> `Contents (c, N.default)
        | Some c, Some m, None -> `Contents (c, m)
        | None, None, Some n -> `Node n
        | _ -> failwith "invalid node" )
    |+ field "contents" (option K.t) (function
         | `Contents (x, _) -> Some x
         | _ -> None )
    |+ field "metadata" (option N.metadata_t) (function
         | `Contents (_, x) when not (equal N.metadata_t N.default x) -> Some x
         | _ -> None )
    |+ field "node" (option K.t) (function `Node n -> Some n | _ -> None)
    |> sealr

  let t : t Type.t =
    Type.map
      Type.(list ~len:`Int64 (pair step_t value_t))
      (fun entries ->
        (* FIXME(samoht): is [fst] ok? *)
        fst (of_tree (tree entries)) )
      (fun (t : t) -> t.entries)
end
