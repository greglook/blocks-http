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

; Thoughts:
; - Stat metadata needs to be communicated in headers.
; - Not clear how a client would use POST via `store!`
; - PUT must validate hash before storing block.
; - Support for DELETE should probably be configurable.


(defn- handle-index-request
  [store request]
  (case (:method request)
    ; GET /blocks/?after=...&limit=...
    :get
      (throw (UnsupportedOperationException. "NYI"))

    ; POST /blocks/
    :post
      (throw (UnsupportedOperationException. "NYI"))

    ; Bad method
    {:status 405
     :headers {"Allow" "GET, POST"}
     :body "Method not allowed on this resource"}))


(defn- handle-block-request
  [store request id]
  (case (:method request)
    ; HEAD /blocks/:id
    :head
      (throw (UnsupportedOperationException. "NYI"))

    ; GET /blocks/:id
    :get
      (throw (UnsupportedOperationException. "NYI"))

    ; PUT /blocks/:id
    :put
      (throw (UnsupportedOperationException. "NYI"))

    ; DELETE /blocks/:id
    :delete
      (throw (UnsupportedOperationException. "NYI"))

    ; Bad method
    {:status 405
     :headers {"Allow" "HEAD, GET, PUT, DELETE"}
     :body "Method not allowed on this resource"}))


(defn ring-handler
  "Constructs a new handler function for REST HTTP requests against the block
  store."
  [store mount-path]
  (fn handler
    [request]
    (when-not (.startsWith ^String (:uri request) mount-path)
      (throw (IllegalStateException.
               (str "Routing failure: handler at " mount-path
                    " received request for " (:uri request)))))
    (let [path-id (subs (:uri request) (count mount-path))]
      (cond
        (= "" path-id)
          (handle-index-request store request)

        (not (.indexOf path-id "/"))
          (let [id-or-err
                (try
                  (multihash/decode path-id)
                  (catch Exception e
                    {:status 400
                     :body (str "Invalid multihash id: " (pr-str path-id)
                                " " (.getMessage e))}))]
            (if (:status id-or-err)
              id-or-err
              (handle-block-request store request id-or-err)))

        :else
          {:status 404
           :body "Not Found"}))))




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
