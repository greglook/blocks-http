(ns blocks.store.http
  "Block storage transport over HTTP."
  (:require
    [blocks.store.http.client :as client]
    [blocks.store.http.server :as server]))


(defn ring-handler
  "Constructs a new handler function for REST HTTP requests against the block
  store."
  [store mount-path]
  (server/ring-handler store mount-path))


(defn http-store
  "Creates a new HTTP block store client."
  [server-url]
  (client/http-store server-url))
