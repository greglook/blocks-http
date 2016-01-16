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
  (case (:request-method request)
    ; GET /blocks/?after=...&limit=...
    :get
      ; TODO: enforce maximum limit
      (let [stats (block/list store (:params request))]
        ; TODO: if truncated, add next link header
        {:status 200
         :headers {}
         :body {:data (mapv #(let [b58-id (multihash/base58 (:id %))]
                               (assoc %
                                      :id b58-id
                                      :url (str (name (:scheme request))
                                                "://" (get-in request [:headers "Host"] "localhost")
                                                ":" 8080
                                                (:uri request) b58-id)))
                            stats)}})

    ; POST /blocks/
    :post
      (throw (UnsupportedOperationException. "NYI: (block/store! store body)"))

    ; Bad method
    {:status 405
     :headers {"Allow" "GET, POST"}
     :body "Method not allowed on this resource"}))


(defn- handle-block-request
  [store request id]
  (case (:request-method request)
    ; HEAD /blocks/:id
    :head
      (throw (UnsupportedOperationException. "NYI: (block/stat store id)"))

    ; GET /blocks/:id
    :get
      (if-let [block (block/get store id)]
        {:status 200
         :headers {"Content-Type" "application/octet-stream"
                   "Content-Size" (str (:size block))}
         :body (block/open block)}
        {:status 404
         :headers {}
         :body "Not Found"})

    ; PUT /blocks/:id
    :put
      (throw (UnsupportedOperationException. "NYI: (block/put! store body)"))

    ; DELETE /blocks/:id
    :delete
      (throw (UnsupportedOperationException. "NYI: (block/delete! store id)"))

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

        (= -1 (.indexOf path-id "/"))
          (let [[id err]
                (try
                  [(multihash/decode path-id) nil]
                  (catch Exception e
                    [nil
                     {:status 400
                      :headers {}
                      :body (str "Invalid multihash id: " (pr-str path-id)
                                " " (.getMessage e))}]))]
            (or err (handle-block-request store request id)))

        :else
          {:status (do (prn path-id) 404)
           :headers {}
           :body "No Matching Resource"}))))




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
