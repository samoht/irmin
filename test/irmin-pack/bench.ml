module Int63 = struct
  include Optint.Int63

  let t : t Irmin.Type.t =
    let open Irmin.Type in
    (map int64) of_int64 to_int64
    |> like ~pp:Optint.Int63.pp ~equal:(stage Optint.Int63.equal)
         ~compare:(stage Optint.Int63.compare)

  module Map = Stdlib.Map.Make (struct
    type nonrec t = t

    let compare = compare
  end)
end

type int63 = Int63.t [@@deriving irmin]

module Conf = struct
  let entries = 32
  let stable_hash = 256
end

open Irmin.Export_for_backends
module Hash = Tezos_context_hash.Hash

module Schema = struct
  module Path = Irmin.Path.String_list
  module Metadata = Irmin.Metadata.None
  module Branch = Irmin.Branch.String
  module Hash = Tezos_context_hash.Hash
  module Info = Irmin.Info.Default

  module Node = struct
    module M = Irmin.Node.Make (Hash) (Path) (Metadata)

    (* [V1] is only used to compute preimage hashes. [assert false]
       statements should be unreachable.*)
    module V1 : sig
      val pre_hash : M.t -> (string -> unit) -> unit
    end = struct
      module Hash = Irmin.Hash.V1 (Hash)

      type entry = string * M.value

      (* Irmin 1.4 uses int8 to store filename lengths.

         Irmin 2 use a variable-size encoding for strings; this is using int8
         for strings of size stricly less than 128 (e.g. 2^7) which happen to
         be the case for all filenames ever produced by Irmin 1.4. *)
      let step_t = Irmin.Type.string

      let metadata_t =
        let some = "\255\000\000\000\000\000\000\000" in
        let none = "\000\000\000\000\000\000\000\000" in
        Irmin.Type.(map (string_of (`Fixed 8)))
          (fun _ -> assert false)
          (function Some _ -> some | None -> none)

      let metadata_of_entry (_, t) =
        match t with `Node _ -> None | `Contents (_, m) -> Some m

      let hash_of_entry (_, t) =
        match t with `Node h -> h | `Contents (h, _) -> h

      (* Irmin 1.4 uses int64 to store list lengths *)
      let entry_t : entry Irmin.Type.t =
        let open Irmin.Type in
        record "Tree.entry" (fun _ _ _ -> assert false)
        |+ field "kind" metadata_t metadata_of_entry
        |+ field "name" step_t fst
        |+ field "hash" Hash.t hash_of_entry
        |> sealr

      let entries_t : entry list Irmin.Type.t =
        Irmin.Type.(list ~len:`Int64 entry_t)

      let pre_hash_entries = Irmin.Type.(unstage (pre_hash entries_t))
      let compare_entry (x, _) (y, _) = String.compare x y
      let step_to_string = Irmin.Type.(unstage (to_bin_string Path.step_t))
      let str_key (k, v) = (step_to_string k, v)

      let pre_hash t =
        M.list t
        |> List.map str_key
        |> List.fast_sort compare_entry
        |> pre_hash_entries
    end

    include M

    let t = Irmin.Type.(like t ~pre_hash:(stage @@ fun x -> V1.pre_hash x))
  end

  module Commit = struct
    module M = Irmin.Commit.Make (Hash)
    module V1 = Irmin.Commit.V1.Make (M)
    include M

    let pre_hash_v1_t = Irmin.Type.(unstage (pre_hash V1.t))
    let pre_hash_v1 t = pre_hash_v1_t (V1.import t)
    let t = Irmin.Type.(like t ~pre_hash:(stage @@ fun x -> pre_hash_v1 x))
  end

  module Contents = struct
    type t = bytes

    let ty = Irmin.Type.(pair (bytes_of `Int64) unit)
    let pre_hash_ty = Irmin.Type.(unstage (pre_hash ty))
    let pre_hash_v1 x = pre_hash_ty (x, ())
    let t = Irmin.Type.(like bytes ~pre_hash:(stage @@ fun x -> pre_hash_v1 x))
    let merge = Irmin.Merge.(idempotent (Irmin.Type.option t))
  end
end

module Maker = Irmin_pack.Maker_ext (Irmin_pack.Version.V1) (Conf)
module Store = Maker.Make (Schema)

type stats = { duration : float; mem : float } [@@deriving irmin]

let pp_path = Repr.pp Schema.Path.t
let hit_count0 = ref 0
let hit_count1 = ref 0

let fold ?depth t k ~init ~f =
  Store.Tree.find_tree t k >>= function
  | None -> Lwt.return init
  | Some t ->
      Store.Tree.fold ?depth ~force:`And_clear ~uniq:`False
        ~node:(fun k v acc -> f k (Store.Tree.of_node v) acc)
        ~contents:(fun k v acc ->
          if k = [] then Lwt.return acc else f k (Store.Tree.of_contents v) acc)
        t init

let fold ?depth tree key ~init ~f = fold ?depth tree key ~init ~f

let flatten ~tree ~key ~depth ~rename ~init =
  fold tree key ~depth:(`Eq depth) ~init ~f:(fun old_key tree dst_tree ->
      incr hit_count1;
      let new_key = rename old_key in
      Store.Tree.add_tree dst_tree new_key tree)
  >>= fun dst_tree -> Store.Tree.add_tree tree key dst_tree

let fold_flatten ctxt abs_key depth' mid_key ~depth ~rename =
  let abs_key = [ "data" ] @ abs_key in
  let+ ctxt =
    fold ~depth:(`Eq depth') ctxt abs_key ~init:ctxt ~f:(fun key tree ctxt ->
        incr hit_count0;
        flatten ~tree ~key:mid_key ~depth ~rename ~init:Store.Tree.empty
        >>= fun tree -> Store.Tree.add_tree ctxt (abs_key @ key) tree)
  in
  ctxt

let fold_flatten ctxt abs_key depth' mid_key ~depth ~rename =
  Fmt.epr "> fold_flatten: %a %a\n%!" pp_path abs_key pp_path mid_key;
  hit_count0 := 0;
  hit_count1 := 0;
  let t = Mtime_clock.counter () in
  let+ res = fold_flatten ctxt abs_key depth' mid_key ~depth ~rename in
  let t = Mtime_clock.count t |> Mtime.Span.to_s in
  let mem = Int64.to_float Rusage.((get Self).maxrss) /. 1e9 in
  Fmt.epr "  %#d / %#d hits, %.3f sec, %.6f GB\n%!" !hit_count0 !hit_count1 t
    mem;

  res

let flatten_storage tree =
  let rec drop n xs =
    match (n, xs) with
    | 0, _ -> xs
    | _, [] -> assert false
    | _, _ :: xs -> drop (n - 1) xs
  in
  fold_flatten tree [ "contracts"; "index" ] 0 [] ~depth:7 ~rename:(drop 6)

let random_ascii () =
  let chars = "0123456789abcdefghijklmnopqrstABCDEFGHIJKLMNOPQRST-_." in
  chars.[Random.int @@ String.length chars]

let random_string n = String.init n (fun _i -> random_ascii ())

let init t n =
  let rec aux t i =
    let s = random_string 20 in
    let k =
      [
        String.sub s 0 2;
        String.sub s 2 2;
        String.sub s 4 2;
        String.sub s 8 2;
        String.sub s 10 2;
        String.sub s 12 2;
        s;
      ]
    in
    if i mod 10_000 = 0 then Fmt.epr "\r%dk/%dk%!" (i / 1000) (n / 1000);
    Store.Tree.add t k (Bytes.of_string s) >>= fun t ->
    if i < n then aux t (i + 1)
    else (
      Fmt.epr "\n%!";
      Lwt.return t)
  in
  aux t 0

let info () = Store.Info.v ~author:"Tezos" ~message:"truc" 42L
let entries = try int_of_string Sys.argv.(1) with _ -> 1_000_000
let path = [ "data"; "contracts"; "index" ]

let work p () =
  Printf.eprintf "************************************************\n%!";
  Printf.eprintf "%s\n%!" p;
  Printf.eprintf "%dk entries\n%!" (entries / 1000);
  let conf = Irmin_pack.config ~readonly:false p in
  let* repo = Store.Repo.v conf in
  Printf.eprintf "> Got repo\n%!";

  Store.clear repo >>= fun () ->
  (* FIXME: bug in AO.remove in irmin-pack *)
  Store.Branch.remove repo "master" >>= fun () ->
  Store.master repo >>= fun t ->
  Store.find_tree t path >>= fun p ->
  assert (p = None);
  Printf.eprintf "> Generate random tree\n%!";

  let time = Mtime_clock.counter () in
  init Store.Tree.empty entries >>= fun tree ->
  let time = Mtime_clock.count time |> Mtime.Span.to_s in
  let mem = Int64.to_float Rusage.((get Self).maxrss) /. 1e9 in
  Fmt.epr "\nInit tree: %.3f sec, %.6f GB\n%!" time mem;

  let time = Mtime_clock.counter () in
  Store.set_tree_exn ~info t path tree >>= fun () ->
  let time = Mtime_clock.count time |> Mtime.Span.to_s in
  let mem = Int64.to_float Rusage.((get Self).maxrss) /. 1e9 in
  Fmt.epr "\nCommit tree: %.3f sec, %.6f GB\n%!" time mem;

  Store.Head.get t >>= fun c ->
  Printf.eprintf "> Got commit\n%!";
  let tree = Store.Commit.tree c in
  let hash = Store.Commit.hash c in

  let time = Mtime_clock.counter () in
  let* tree = flatten_storage tree in
  let time = Mtime_clock.count time |> Mtime.Span.to_s in
  let mem = Int64.to_float Rusage.((get Self).maxrss) /. 1e9 in
  Fmt.epr "\nFlatten: %.3f sec, %.6f GB\n%!" time mem;

  (* let time = Mtime_clock.counter () in
   * Gc.compact ();
   * let time = Mtime_clock.count time |> Mtime.Span.to_s in
   * let mem = Int64.to_float Rusage.((get Self).maxrss) /. 1e9 in
   * Fmt.epr "\n%.3f sec, %.6f GB\n%!" time mem; *)
  let parents = [ hash ] in

  let time = Mtime_clock.counter () in
  let* _h = Store.Commit.v repo ~info:(info ()) ~parents tree in
  let time = Mtime_clock.count time |> Mtime.Span.to_s in
  let mem = Int64.to_float Rusage.((get Self).maxrss) /. 1e9 in
  Fmt.epr "Commit: %.3f sec, %.6f GB\n%!" time mem;

  let time = Mtime_clock.counter () in
  Store.Tree.clear tree;
  let time = Mtime_clock.count time |> Mtime.Span.to_s in
  let mem = Int64.to_float Rusage.((get Self).maxrss) /. 1e9 in
  Fmt.epr "Clear: %.3f sec, %.6f GB\n%!" time mem;

  let time = Mtime_clock.counter () in
  let* () = Store.Repo.close repo in
  let time = Mtime_clock.count time |> Mtime.Span.to_s in
  let mem = Int64.to_float Rusage.((get Self).maxrss) /. 1e9 in
  Fmt.epr "Close: %.3f sec, %.6f GB\n%!" time mem;

  Lwt.return ()

let () = Memtrace.trace_if_requested ~context:"bench" ()

let ignore_srcs src =
  List.mem (Logs.Src.name src)
    [
      "git.inflater.decoder";
      "git.deflater.encoder";
      "git.encoder";
      "git.decoder";
      "git.loose";
      "git.store";
      "cohttp.lwt.io";
    ]

let reporter ?(prefix = "") () =
  let pad n x =
    if String.length x > n then x
    else x ^ Astring.String.v ~len:(n - String.length x) (fun _ -> ' ')
  in
  let report src level ~over k msgf =
    let k _ =
      over ();
      k ()
    in
    let ppf = match level with Logs.App -> Fmt.stdout | _ -> Fmt.stderr in
    let with_stamp h _tags k fmt =
      let dt = Mtime.Span.to_us (Mtime_clock.elapsed ()) in
      Fmt.kpf k ppf
        ("%s%+04.0fus %a %a @[" ^^ fmt ^^ "@]@.")
        prefix dt
        Fmt.(styled `Magenta string)
        (pad 15 @@ Logs.Src.name src)
        Logs_fmt.pp_header (level, h)
    in
    msgf @@ fun ?header ?tags fmt ->
    if ignore_srcs src then Format.ikfprintf k ppf fmt
    else with_stamp header tags k fmt
  in
  { Logs.report }

let () =
  try
    if Sys.getenv "DEBUG_BENCH" <> "" then (
      Logs.set_level (Some Logs.Debug);
      Logs.set_reporter (reporter ()))
  with Not_found -> ()

let main = work "./tezos-context-bench" ()
let () = Lwt_main.run main
