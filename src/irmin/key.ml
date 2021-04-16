include Key_intf

module Make (H : Hash.S) (V : Type.S) = struct
  module Hash = H

  type hash = H.t
  type metadata = V.t
  type t = { hash : H.t; mutable metadata : V.t option }

  let t : t Type.t =
    Type.map H.t (fun hash -> { hash; metadata = None }) (fun t -> t.hash)

  let pp_hash = Type.pp H.t
  let pp_id = Type.pp V.t

  let dump ppf t =
    match t.metadata with
    | None -> Fmt.pf ppf "[%a]" pp_hash t.hash
    | Some id -> Fmt.pf ppf "[%a:%a]" pp_hash t.hash pp_id id

  let hash t = t.hash
  let metadata t = t.metadata
  let v ?metadata hash = { hash; metadata }
  let clear t = t.metadata <- None
  let set t m = t.metadata <- Some m
end

module Hash (H : Hash.S) = struct
  module Hash = H

  type t = H.t [@@deriving irmin]
  type metadata = unit
  type hash = Hash.t

  let dump = Type.pp H.t
  let hash x = x
  let metadata _ = Some ()
  let v ?metadata:_ x = x
end
