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
  (let [[success body] (if (and (< 1 (count body))
                                (integer? (first body)))
                         [(first body) (rest body)]
                         [200 body])]
    `(let [response# ~request]
       (condp = (:status response#)
         ; Successful response.
         ~success
         (let [~'response response#]
           ~@body)

         ; Not found.
         404 nil

         ; Some other error status.
         (throw (ex-info (str "Unsuccessful blocks-http response: "
                              (:status response#) " - "
                              (:error (:body response#) "--"))
                         {:expected ~success
                          :status (:status response#)
                          :headers (:headers response#)
                          :body (:body response#)}))))))



;; ## HTTP Block Client

(defrecord HTTPBlockClient
  [server-url]

  store/BlockStore

  (-stat
    [this id]
    (when id
      (do-request (http/head (block-url server-url id)
                             {:throw-exceptions false})
        (-> (:headers response)
            (util/header-stats)
            (assoc :id id)))))


  (-list
    [this opts]
    (do-request (http/get server-url
                          {:query-params opts
                           :throw-exceptions false
                           :accept :edn
                           :debug true
                           :as :edn})
      ; TODO: lazy seq a-la s3
      ; TODO: parse :id and :stored-at
      (->> (:body response)
           (:data)
           (map #(-> % (update :id multihash/decode)
                       (update :stored-at util/parse-date))))))


  (-get
    [this id]
    (when-let [stats (.-stat this id)]
      ; TODO: return a lazy block instead?
      (do-request (http/get (block-url server-url id)
                            {:throw-exceptions false})
        (block/with-stats
          (block/read! (:body response))
          (util/header-stats (:headers response))))))


  (-put!
    [this block]
    (when block
      (do-request (http/put (block-url server-url (:id block))
                            {:body (block/open block)
                             :length (:size block)
                             :content-type "application/octet-stream"
                             :throw-exceptions false})
        204
        (block/with-stats
          block
          (dissoc (util/header-stats (:headers response)) :size)))))


  (-delete!
    [this id]
    (when id
      (throw (UnsupportedOperationException. "NYI"))
      (do-request (http/delete (block-url server-url id))
        ; TODO: parse response for successful deletion
        ))))


(defn http-store
  "Creates a new HTTP block store client."
  [server-url]
  (HTTPBlockClient. server-url))


;; Remove automatic constructor functions.
(ns-unmap *ns* '->HTTPBlockClient)
(ns-unmap *ns* 'map->HTTPBlockClient)
