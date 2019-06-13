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

  let kind : kind Type.t =
    let open Type in
    variant "Tree.kind" (fun node contents contents_m -> function
      | `Node -> node
      | `Contents m ->
          if Type.equal M.t m M.default then contents else contents_m m )
    |~ case0 "node" `Node
    |~ case0 "contents" (`Contents M.default)
    |~ case1 "contents" M.t (fun m -> `Contents m)
    |> sealv

  let entry : entry Type.t =
    let open Type in
    record "Tree.entry" (fun kind name node -> { kind; name; node })
    |+ field "kind" kind (function { kind; _ } -> kind)
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

  module StepMap = struct
    include Map.Make (struct
      type t = P.step

      let compare = Type.compare P.step_t
    end)

    let of_list l = List.fold_left (fun acc (k, v) -> add k v acc) empty l
  end

  type value = [ `Contents of hash * metadata | `Node of hash ]

  type node = Entries of entry StepMap.t | Inodes of v array

  and v = Empty | Direct of inode | Indirect of hash

  and inode = { level : int; node : node }

  type t = {
    level : int;
    entries : (step * entry) list;
    inodes : (int * hash) list
  }

  let empty : t = { level = 0; entries = []; inodes = [] }

  let t : t Type.t =
    let open Type in
    record "Node.t" (fun level entries inodes -> { level; entries; inodes })
    |+ field "level" int (fun t -> t.level)
    |+ field "entries" (list (pair P.step_t entry)) (fun t -> t.entries)
    |+ field "inodes" (list (pair int K.t)) (fun t -> t.inodes)
    |> sealr

  let entries =
    let open Type in
    map (list (pair P.step_t entry)) StepMap.of_list StepMap.bindings

  let node (x : v Type.t) : node Type.t =
    let open Type in
    variant "Node.node" (fun entries inodes -> function
      | Entries e -> entries e | Inodes i -> inodes i )
    |~ case1 "Entries" entries (fun e -> Entries e)
    |~ case1 "Inodes" (array x) (fun n -> Inodes n)
    |> sealv

  let v (x : inode Type.t) : v Type.t =
    let open Type in
    variant "Node.v" (fun empty direct indirect -> function
      | Empty -> empty | Direct x -> direct x | Indirect x -> indirect x )
    |~ case0 "Empty" Empty
    |~ case1 "Direct" x (fun i -> Direct i)
    |~ case1 "Indirect" K.t (fun h -> Indirect h)
    |> sealv

  let inode (x : node Type.t) : inode Type.t =
    let open Type in
    record "Node.inode" (fun level node -> { level; node })
    |+ field "level" int (fun (t : inode) -> t.level)
    |+ field "node" x (fun t -> t.node)
    |> sealr

  let inode_t : inode Type.t =
    Type.mu (fun x ->
        let v = v x in
        let node = node v in
        inode node )

  let of_inode v =
    let nodes = ref [] in
    let rec aux k (v : inode) =
      match v.node with
      | Entries m ->
          k { level = v.level; entries = StepMap.bindings m; inodes = [] }
      | Inodes n ->
          let _, entries, inodes =
            Array.fold_left
              (fun (i, entries, inodes) -> function
                | Empty -> (i + 1, entries, inodes)
                | Indirect h -> (i + 1, entries, (i, h) :: inodes)
                | Direct n -> (
                  match n.node with
                  | Entries m when StepMap.is_empty m ->
                      (i + 1, entries, inodes)
                  | Entries m when StepMap.cardinal m = 1 ->
                      let s, e = StepMap.choose m in
                      (i + 1, (s, e) :: entries, inodes)
                  | _ ->
                      let n = aux k n in
                      let k = K.digest (Type.pre_digest t n) in
                      nodes := n :: !nodes;
                      (i + 1, entries, (i, k) :: inodes) ) )
              (0, [], []) n
          in
          k { level = v.level; entries; inodes }
    in
    let n = aux (fun x -> x) v in
    (n, !nodes)

  let max_entries = 64

  let max_inodes = 64

  let to_inode (t : t) : inode =
    assert (List.length t.entries <= max_entries);
    assert (List.length t.inodes <= max_inodes);
    if t.inodes = [] then
      { level = t.level; node = Entries (StepMap.of_list t.entries) }
    else
      let node = Array.make max_inodes Empty in
      List.iter (fun (i, h) -> node.(i) <- Indirect h) t.inodes;
      { level = t.level; node = Inodes node }

  let index ~level k = abs (Type.hash P.step_t ~seed:level k) mod max_inodes

  let v (l : (step * value) list) : inode =
    let rec aux (k : inode -> unit) level entries =
      if StepMap.cardinal entries <= max_entries then
        k { level; node = Entries entries }
      else
        let inodes =
          Array.make max_inodes
            (Direct { level; node = Entries StepMap.empty })
        in
        StepMap.iter
          (fun k v ->
            let i = index ~level k in
            match inodes.(i) with
            | Direct { node = Entries m; _ } ->
                inodes.(i)
                <- Direct { level; node = Entries (StepMap.add k v m) }
            | _ -> assert false )
          entries;
        Array.iteri
          (fun i -> function
            | Direct { node = Entries m; _ } ->
                if StepMap.cardinal m <= max_entries then ()
                else aux (fun n -> inodes.(i) <- Direct n) (level + 1) m
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

  let list ~find t =
    let rec aux acc t =
      match t.node with
      | Entries t -> Lwt.return (StepMap.union (fun _ _ -> assert false) acc t)
      | Inodes n ->
          Lwt_list.fold_left_s
            (fun acc -> function Empty -> Lwt.return acc
              | Direct n -> aux acc n
              | Indirect h -> (
                  find h >>= function
                  | None -> Lwt.return acc
                  | Some n -> aux acc (to_inode n) ) )
            acc (Array.to_list n)
    in
    aux StepMap.empty t >|= list_entries

  let find_entries t s =
    try
      let _, v = of_entry (StepMap.find s t) in
      Some v
    with Not_found -> None

  let find ~find t s =
    let rec aux t =
      match t.node with
      | Entries t -> Lwt.return (find_entries t s)
      | Inodes n -> (
        match n.(index ~level:t.level s) with
        | Empty -> Lwt.return None
        | Direct n -> aux n
        | Indirect h -> (
            find h >>= function
            | None -> Lwt.return None
            | Some n -> aux (to_inode n) ) )
    in
    aux t

  let is_empty (t : inode) =
    match t.node with Entries m -> StepMap.is_empty m | Inodes _ -> false

  let update ~find t k x =
    (* FIXME(samoht): not optimized at all *)
    list ~find t >|= fun entries -> v ((k, x) :: entries)

  let remove ~find t k =
    (* FIXME(samoht): not optimized at all *)
    list ~find t >|= fun entries -> v (List.remove_assoc k entries)

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
    Type.map Type.(list entry) of_entries to_entries
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

  let contents_t = C.Key.t

  let metadata_t = M.t

  let step_t = Path.step_t

    let promise_contents old =
           Merge.bind_promise old (function
               | Some (`Contents (_, x, y)) -> Merge.promise (Some (x, y))
               | _ -> Merge.promise None )

    let promise_node old =
      Merge.bind_promise old (function
          | Some (`Node (_, x)) -> Merge.promise (Some x)
          | _ -> Merge.promise None )

    let promise_inode old =
      Merge.bind_promise old (function
          | Some (`Inode (_, x)) -> Merge.promise (Some x)
          | _ -> Merge.promise None )


    let merge_value t merge_contents merge =

      let merge_contents = Merge.(f (option (pair merge_contents M.merge))) in

      let merge : S.Val.value option Merge.f =

     fun ~old x y ->
      match (x, y) with
      | None, None -> Lwt.return (Ok None)
      | Some x, None | None, Some x ->
        (match x with
         | `Contents (l, a, b) ->
           let old = promise_contents old in
           Merge.
      | Some x, Some y -> (
        match (x, y) with
          | `Contents (l, a, b), `Contents (_, c, d) -> (
              let old = promise_contents old in
            merge_contents ~old
                (Some (a, b))
                (Some (c, d)))
            >|= function
            | Ok (Some (a, b)) -> Ok (Some (`Contents (l, a, b)))
            | Ok None -> Ok None
            | Error _ as e -> e )
        | `Node (l, a), `Node (_, b) -> (
            let old =
              Merge.bind_promise old (function
                | Some (`Node (_, x)) -> Merge.promise (Some x)
                | _ -> Merge.promise None )
            in
            Merge.(f (merge t) ~old (Some a) (Some b)) >|= function
            | Ok (Some a) -> Ok (Some (`Node (l, a)))
            | Ok None -> Ok None
            | Error _ as e -> e )
        | `Inode (l, a), `Inode (_, b) -> (
            let old =
            in
            Merge.(f (merge t) ~old (Some a) (Some b)) >|= function
            | Ok (Some a) -> Ok (Some (`Inode (l, a)))
            | Ok None -> Ok None
            | Error _ as e -> e )
        | `Contents _, `Node _ | `Node _, `Contents _ ->
            Merge.conflict "contents/node"
        | `Contents _, `Inode _ | `Inode _, `Contents _ ->
            Merge.conflict "contents/inode"
        | `Node _, `Inode _ | `Inode _, `Node _ -> Merge.conflict "node/inode"
        )
    in
    Merge.v (Type.option S.Val.value_t) merge

  let merge_values t merge_key merge =
    Merge.alist step_t S.Val.value_t (fun _ ->
        Merge.option @@ merge_value t merge_key merge )

  let rec merge t =
    let merge_key =
      Merge.v S.Key.t (fun ~old x y -> Merge.(f (merge t)) ~old x y)
    in
    let empty = S.Val.empty in
    let merge = merge_value t merge_key merge in
    let read k = find t k >|= function None -> empty | Some v -> v in
    let add v = add t v >>= fun k -> Lwt.return k in
    Merge.like_lwt Type.(option S.Key.t) merge read add

  module Val = S.Val
end

module Helpers (N : S.NODE_STORE) = struct
  let rec list _t n =
    Lwt_list.map_s
      (function `Value (_, v) -> Lwt.return v | `Inode _ -> assert false)
      (N.Val.list (N.Val.to_inode n))

  let find _t _n _s = failwith "TODO"

  let update _t _n _s _v = failwith "TODO"

  let remove _t _n _s = failwith "TODO"
end

module Graph (S : S.NODE_STORE) = struct
  module Path = S.Path
  module Contents = S.Contents.Key
  module Metadata = S.Metadata
  module Node = Helpers (S)

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
    find n >>= function None -> Lwt.return [] | Some n -> Node.list t n

  module U = struct
    type t = unit

    let t = Type.unit
  end

  module Graph = Object_graph.Make (Contents) (Metadata) (S.Key) (U) (U)

  let edges db t =
    Node.list db t >|= fun children ->
    List.fold_left
      (fun acc -> function `Node n -> `Node n :: acc
        | `Contents c -> `Contents c :: acc )
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
    S.find t node >>= function
    | None -> Lwt.return None
    | Some n -> Node.find t n step

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
    Node.find t node label >>= fun old_key ->
    ( match old_key with
    | None | Some (`Contents _) -> Lwt.return S.Val.empty
    | Some (`Node k) -> (
        S.find t k >|= function None -> S.Val.empty | Some v -> v ) )
    >>= fun old_node ->
    let old_node = S.Val.to_inode old_node in
    f old_node >>= fun new_node ->
    if old_node == new_node then Lwt.return node
    else if S.Val.is_empty new_node then
      Node.remove t node label >|= fun node ->
      if S.Val.is_empty node then S.Val.(to_inode empty) else node
    else
      let root, rest = S.Val.of_inode new_node in
      Lwt_list.map_p (S.add t) rest >>= fun _ ->
      S.add t root >>= fun k -> Node.update t node label (`Node k)

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

  type substep = N.substep

  type hash = N.hash

  type metadata = N.metadata

  type value = N.value

  let hash_t = N.hash_t

  let metadata_t = N.metadata_t

  type 'a with_entries = {
    v : 'a;
    entries : [ `Value of step * value | `Inode of substep * hash ] list
  }

  type inode = N.inode with_entries

  type t = N.t with_entries

  let of_inode (v : inode) : t * t list =
    let x, y = N.of_inode v.v in
    ( { v = x; entries = v.entries },
      List.map
        (fun v -> { v; entries = [] (* FIXME(samoht): looks wrong...*) })
        y )

  let to_inode (t : t) : inode =
    let v = N.to_inode t.v in
    { v; entries = t.entries }

  let internals t = (t, [])

  let import v = { v; entries = N.list v }

  let export t = t.v

  let v entries : inode =
    let v = N.v entries in
    let entries =
      List.map
        (function `Value v -> `Value v | `Inode (_, h) -> `Inode h)
        entries
    in
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
        fst (of_inode (v entries)) )
      (fun (t : t) -> t.entries)

  let inode_t = Type.map N.inode_t (fun v -> { v; entries = [] }) export
end
