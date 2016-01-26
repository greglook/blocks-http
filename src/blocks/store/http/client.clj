(ns blocks.store.http.client
  "Client implementing a block storage API which communicates with a remote
  store using the HTTP API."
  (:require
    [clj-http.client :as http]
    (blocks
      ;[core :as block]
      [store :as store])
    [blocks.store.http.util :as util]
    [multihash.core :as multihash]))


(defn- block-url
  [server-url id]
  (str server-url (multihash/base58 id)))



;; ## HTTP Block Client

(defrecord HTTPBlockClient
  [server-url]

  store/BlockStore

  (-stat
    [this id]
    (when id
      (let [response (http/head (block-url server-url id))]
        (condp = (:status response)
          ; Successful response.
          200 (assoc (util/header-stats (:headers response))
                     :id id)

          ; Block wasn't found.
          404 nil

          ; Some other status.
          (throw (ex-info (str "Unsuccessful blocks-http response: "
                               (:status response) " - "
                               (:error (:body response) "--"))
                          (:body response)))))))


  (-list
    [this opts]
    (let [response (http/get server-url {:query-params opts
                                  ;:debug true
                                  :as :json})]
      (when (not= 200 (:status response))
        (throw (ex-info (str "Unsuccessful blocks-http response: "
                             (:status response) " - "
                             (:error (:body response) "--"))
                        (:body response))))
      ; TODO: lazy seq a-la s3
      ; TODO: parse :id and :stored-at
      (:data (:body response))))


  (-get
    [this id]
    (when-let [stats (.-stat this id)]
      ; TODO: return a lazy block
      (throw (UnsupportedOperationException. "NYI"))))


  (-put!
    [this block]
    (when block
      (throw (UnsupportedOperationException. "NYI"))
      ; TODO: store block content
      (let [response (http/put (block-url server-url (:id block)))]
        ; TODO: PUT /:id
        )))


  (-delete!
    [this id]
    (when id
      (throw (UnsupportedOperationException. "NYI"))
      (let [response (http/delete (block-url server-url id))]
        ; TODO: parse response for successful deletion
        ))))


(defn http-store
  "Creates a new HTTP block store client."
  [server-url]
  (HTTPBlockClient. server-url))


;; Remove automatic constructor functions.
(ns-unmap *ns* '->HTTPBlockClient)
(ns-unmap *ns* 'map->HTTPBlockClient)
