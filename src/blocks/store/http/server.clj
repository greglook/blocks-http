(ns blocks.store.http.server
  "HTTP request handler which can serve an API for operations against another
  block store."
  (:require
    (blocks
      [core :as block]
      [store :as store])
    [blocks.store.http.util :as util]
    [clojure.string :as str]
    [clojure.tools.logging :as log]
    [multihash.core :as multihash]))


; Thoughts:
; - Stat metadata needs to be communicated in headers.
; - Not clear how a client would use POST via `store!`
; - PUT must validate hash before storing block.
; - Support for DELETE should probably be configurable.



;; ## Helper Functions

(defn- parse-multihash
  "Parse the given string as a multihash - if the parsing succeeds, calls the
  given function with the id. Otherwise, returns a 400 bad request."
  [f str-id]
  (let [[id err]
        (try
          [(multihash/decode str-id) nil]
          (catch Exception e
            [nil
             {:status 400
              :headers {}
              :body (str "Invalid multihash id: " (pr-str str-id)
                        " " (.getMessage e))}]))]
    (or err (f id))))


(defn- request-url
  "Return the full string url a request was made for."
  [request]
  (str (name (:scheme request))
    "://" (:server-name request)
    ":" (:server-port request)
    (:uri request)))


(defn- not-found
  "Returns a 404 Not Found response map with the given body."
  [body]
  {:status 404
   :headers {}
   :body body})


(defn- method-not-allowed
  "Returns a 405 Method Not Allowed response map. The response will indicate
  the allowed methods with a header."
  [allowed-methods]
  {:status 405
   :headers {"Allow" (str/join ", " (map (comp str/upper-case name) allowed-methods))}
   :body "Method not allowed on this resource"})



;; ## Handler Functions

(defn- handle-list
  "Handles a request to list the stored blocks."
  [store request]
  ; TODO: enforce maximum limit
  (let [stats (block/list store (:params request))]
    ; TODO: if truncated, add next link header
    {:status 200
     :headers {}
     :body {:data (mapv #(let [b58-id (multihash/base58 (:id %))]
                           (assoc % :id b58-id :url (str (request-url request) b58-id)))
                        stats)}}))


(defn- handle-store!
  "Handles a request to store a new block."
  [store request]
  (let [size (:content-length request)]
    (if (and size (pos? size))
      (let [block (block/store! store (:body request))]
        {:status 201
         :headers {"Location" (str (request-url request) (multihash/base58 (:id block)))}
         :body {:id (multihash/base58 (:id block))
                :size (:size block)
                :stored-at (util/format-date (:stored-at (block/meta-stats block)))}})
      {:status 411
       :headers {}
       :body "Cannot store block with no content"})))


(defn- handle-stat
  [store request id]
  (if-let [stats (block/stat store id)]
    {:status 200
     :headers (util/block-headers stats)
     :body ""}
    (not-found nil)))


(defn- handle-get
  [store request id]
  (if-let [block (block/get store id)]
    {:status 200
     :headers (util/block-headers block)
     ; TODO: support ranged-open (Accept-Ranges: bytes / Content-Range: bytes 21010-47021/47022)
     :body (block/open block)}
    (not-found (str "Block " id " not found in store"))))


(defn- handle-put!
  [store request id]
  (let [block (block/read! (:body request))]
    ; TODO: validate Content-Length header, reject with 411
    (if (not= id (:id block))
      {:status 400
       :headers {}
       :body (format "Request body %s does not match requested hash %s"
                     (:id block) id)}
      (let [stored (block/put! store block)]
        {:status 204
         :headers {"Last-Modified" (util/format-date (:stored-at (block/meta-stats stored)))}
         :body nil}))))


(defn- handle-delete!
  [store request id]
  (if (block/delete! store id)
    {:status 204
     :headers {}
     :body (str "Block " id " deleted")}
    (not-found (str "Block " id " not found in store"))))



;; ## Routing Functions

(defn- route-collection-request
  [store request]
  (case (:request-method request)
    ; GET /blocks/?after=...&limit=...
    :get  (handle-list store request)
    ; POST /blocks/
    :post (handle-store! store request)
    ; Bad method
    (method-not-allowed [:get :post])))


(defn- route-block-request
  [store request id]
  ; TODO: cache-control headers
  (case (:request-method request)
    ; HEAD /blocks/:id
    :head (handle-stat store request id)
    ; GET /blocks/:id
    :get (handle-get store request id)
    ; PUT /blocks/:id
    :put (handle-put! store request id)
    ; DELETE /blocks/:id
    :delete (handle-delete! store request id)
    ; Bad method
    (method-not-allowed [:head :get :put :delete])))


(defn ring-handler
  "Constructs a new handler function for REST HTTP requests against the block
  store."
  [store mount-path]
  (fn handler
    [request]
    (when-not (str/starts-with? (:uri request) mount-path)
      (throw (IllegalStateException.
               (str "Routing failure: handler at " mount-path
                    " received request for " (:uri request)))))
    (let [path-id (subs (:uri request) (count mount-path))]
      (cond
        (= "" path-id)
          (route-collection-request store request)

        (= -1 (.indexOf path-id "/"))
          (parse-multihash (partial route-block-request store request)
                           path-id)

        :else
          (not-found "No matching resource")))))
