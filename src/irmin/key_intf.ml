module type Abstract = sig
  type t
  type hash
  type metadata

  val dump : t Fmt.t
  (** [dump] is a pretty-printer for [t]. It can show "abstract" information
      such as the id. *)

  val hash : t -> hash
  (** [hash t] it [t]'s hash. *)

  val v : ?metadata:metadata -> hash -> t
  (** [v ?id h] is the key with hash [h] and ID [id] (optionally). *)

  val metadata : t -> metadata option
  (** [metadata t] is [t]'s metadata if it has been provided to [v]. *)
end

module type S = sig
  include Abstract
  (** @inline *)

  include Type.S with type t := t
  (** @inline *)

  module Hash : Hash.S with type t = hash
end

module type Maker = functor (H : Hash.S) (V : Type.S) -> S with type hash = H.t

module type Sigs = sig
  module type S = S
  module type Abstract = Abstract
  module type Maker = Maker

  module Make (H : Hash.S) (V : Type.S) : sig
    include S with type hash = H.t and type metadata = V.t

    val set : t -> metadata -> unit
    val clear : t -> unit
  end

  module Hash (H : Hash.S) : S with type hash = H.t and type metadata = unit
end
