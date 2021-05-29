(*
 * Copyright (c) 2018-2021 Tarides <contact@tarides.com>
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

module type Maker = sig
  type endpoint = unit

  module Make (Schema : Irmin.Schema.S) :
    S.S (* FIXME: we just want to "forget" about node equality *)
      with type Schema.hash = Schema.hash
       and type Schema.branch = Schema.branch
       and type Schema.info = Schema.info
       and type Schema.commit = Schema.commit
       and type Schema.metadata = Schema.metadata
       and type Schema.step = Schema.step
       and type Schema.path = Schema.path
       and type Schema.contents = Schema.contents
       and type Private.Remote.endpoint = endpoint
end

module type Sigs = sig
  module type Maker = Maker
end
