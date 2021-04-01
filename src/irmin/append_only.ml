(*
 * Copyright (c) 2013-2021 Thomas Gazagnaire <thomas@gazagnaire.org>
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

include Append_only_intf

module Wrap_close (S : S) = struct
  include Read_only.Wrap_close (S)

  let s t = fst (raw t)

  let add t k v =
    check_not_closed t;
    S.add (s t) k v

  let batch t f =
    check_not_closed t;
    let t, closed = raw t in
    S.batch t (fun t ->
        (* [closed] ensures that the batch operations fail whenever
           [t] is closed (even concurrently). *)
        f (v ~closed t))

  let clear t =
    check_not_closed t;
    S.clear (s t)
end
