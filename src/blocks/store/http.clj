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

(defn- format-date
  [^java.util.Date date]
  (when date
    (-> (java.text.SimpleDateFormat. "EEE, dd MMM YYYY HH:mm:ss Z")
        (doto (.setTimeZone (java.util.TimeZone/getTimeZone "GMT")))
        (.format date))))


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
                                                "://" (:server-name request)
                                                ":" (:server-port request)
                                                (:uri request) b58-id)))
                            stats)}})

    ; POST /blocks/
    :post
      (if-let [size (:content-length (doto request prn))]
        (if (pos? size)
          (if-let [block (block/store! store (:body request))]
            {:status 201
             :headers {"Location" (str "/" (multihash/base58 (:id block)))} ; TODO: actual prefix
             :body {:id (multihash/base58 (:id block))
                    :size (:size block)}})
          {:status 411
           :headers {}
           :body "Cannot store block with no content"}))

    ; Bad method
    {:status 405
     :headers {"Allow" "GET, POST"}
     :body "Method not allowed on this resource"}))


(defn- handle-block-request
  [store request id]
  (case (:request-method request)
    ; HEAD /blocks/:id
    :head
      (if-let [stats (block/stat store id)]
        {:status 200
         :headers {"Content-Length" (str (:size stats))
                   "Last-Modified" (format-date (:stored-at stats))}
         :body nil}
        {:status 404
         :headers {}
         :body nil})

    ; GET /blocks/:id
    :get
      (if-let [block (block/get store id)]
        {:status 200
         :headers {"Content-Type" "application/octet-stream"
                   "Content-Length" (str (:size block))
                   "Last-Modified" (format-date (:stored-at (block/meta-stats block)))}
         ; TODO: support ranged-open (Accept-Ranges: bytes / Content-Range: bytes 21010-47021/47022)
         :body (block/open block)}
        {:status 404
         :headers {}
         :body (str "Block " id " not found in store")})

    ; PUT /blocks/:id
    :put
      (let [block (block/read! (:body request))]
        ; TODO: validate Content-Length header, reject with 411
        (if (not= id (:id block))
          {:status 400
           :headers {}
           :body (format "Request body %s does not match requested hash %s"
                         (:id block) id)}
          (let [stored (block/put! store block)]
            {:status 204
             :headers {"Last-Modified" (format-date (:stored-at (block/meta-stats stored)))}
             :body nil})))

    ; DELETE /blocks/:id
    :delete
      (if (block/delete! store id)
        {:status 204
         :headers {}
         :body (str "Block " id " deleted")}
        {:status 404
         :headers {}
         :body (str "Block " id " not found in store")})

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
