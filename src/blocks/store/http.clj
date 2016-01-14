(ns blocks.store.http
  "Block storage transport over HTTP."
  (:require
    (blocks
      [core :as block]
      [data :as data]
      [store :as store])
    [blocks.store.util :as util]
    [clojure.string :as str]
    [multihash.core :as multihash]))


;; ## HTTP Block Server

; TODO: ring-compatible handler backed by another store

; Say the handler was mounted at /blocks
; GET    /blocks/?after=...&limit=...    (block/list store opts)
; POST   /blocks/                        (block/store! store body)
; HEAD   /blocks/:id                     (block/stat store id)
; GET    /blocks/:id                     (block/get store id)
; PUT    /blocks/:id                     (block/put! store body)
; DELETE /blocks/:id                     (block/delete! store id)

; Thoughts:
; - Stat metadata needs to be communicated in headers.
; - Not clear how a client would use POST via `store!`
; - PUT must validate hash before storing block.
; - Support for DELETE should probably be configurable.



;; ## HTTP Block Client

(defrecord HTTPBlockClient
  [server-url]

  store/BlockStore

  (-stat
    [this id]
    (throw (UnsupportedOperationException. "NYI")))


  (-list
    [this opts]
    (throw (UnsupportedOperationException. "NYI")))


  (-get
    [this id]
    (throw (UnsupportedOperationException. "NYI")))


  (-put!
    [this block]
    (throw (UnsupportedOperationException. "NYI")))


  (-delete!
    [this id]
    (throw (UnsupportedOperationException. "NYI"))))


(defn http-store
  [server-url]
  (HTTPBlockClient. server-url))


;; Remove automatic constructor functions.
(ns-unmap *ns* '->HTTPBlockClient)
(ns-unmap *ns* 'map->HTTPBlockClient)
