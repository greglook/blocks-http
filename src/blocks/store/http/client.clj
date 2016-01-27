(ns blocks.store.http.client
  "Client implementing a block storage API which communicates with a remote
  store using the HTTP API."
  (:require
    [clj-http.client :as http]
    (blocks
      [core :as block]
      [data :as data]
      [store :as store])
    [blocks.store.http.util :as util]
    [multihash.core :as multihash]))


(defn- block-url
  [server-url id]
  (str server-url (multihash/base58 id)))


(defmacro ^:private do-request
  [request & body]
  `(let [response# (http/request ~(assoc request :throw-exceptions false))]
     (cond
       ; Successful response.
       (http/success? response#)
         (let [~'response response#]
           ~@body)

       ; Not found.
       (http/missing? response#)
         nil

       ; Some other error status.
       :else
         (throw (ex-info (str "Unsuccessful blocks-http response: "
                              (:status response#) " - "
                              (:body response#))
                         {:status (:status response#)
                          :headers (:headers response#)
                          :body (:body response#)})))))



;; ## HTTP Block Client

(defrecord HTTPBlockClient
  [server-url]

  store/BlockStore

  (-stat
    [this id]
    (when id
      (do-request {:method :head
                   :url (block-url server-url id)}
        (-> (:headers response)
            (util/header-stats)
            (assoc :id id)))))


  (-list
    [this opts]
    (binding [*data-readers* (assoc *data-readers* 'data/hash multihash/decode)]
      (do-request {:method :get
                   :url server-url
                   :query-params opts
                   :accept :edn
                   :as :clojure}
        ; TODO: lazy seq a-la s3
        (:data (:body response)))))


  (-get
    [this id]
    (when id
      ; TODO: return a lazy block instead?
      (do-request {:method :get
                   :url (block-url server-url id)}
        (block/with-stats
          (block/read! (:body response))
          (util/header-stats (:headers response))))))


  (-put!
    [this block]
    (when block
      (do-request {:method :put
                   :url (block-url server-url (:id block))
                   :body (block/open block)
                   :length (:size block)
                   :content-type "application/octet-stream"}
        (block/with-stats
          block
          (dissoc (util/header-stats (:headers response)) :size)))))


  (-delete!
    [this id]
    (when id
      (boolean
        (do-request {:method :delete
                     :url (block-url server-url id)}
          true)))))


(defn http-store
  "Creates a new HTTP block store client."
  [server-url]
  (HTTPBlockClient. server-url))


;; Remove automatic constructor functions.
(ns-unmap *ns* '->HTTPBlockClient)
(ns-unmap *ns* 'map->HTTPBlockClient)
