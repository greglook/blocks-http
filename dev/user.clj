(ns user
  "Custom repl customization for local development."
  (:require
    [blocks.core :as block]
    (blocks.store
      [memory :refer [memory-store]]
      [http :refer [http-store ring-handler]]
      [tests :as tests])
    [clojure.java.io :as io]
    [clojure.repl :refer :all]
    [clojure.string :as str]
    [multihash.core :as multihash]
    [ring.adapter.jetty :as jetty]
    (ring.middleware
      [content-type :refer [wrap-content-type]]
      [format :refer [wrap-restful-format]]
      [keyword-params :refer [wrap-keyword-params]]
      [params :refer [wrap-params]]))
  (:import
    blocks.data.Block
    multihash.core.Multihash
    org.eclipse.jetty.server.Server))


; Conditional imports from clj-stacktrace and clojure.tools.namespace:
(try (require '[clojure.stacktrace :refer [print-cause-trace]]) (catch Exception e nil))
(try (require '[clojure.tools.namespace.repl :refer [refresh]]) (catch Exception e nil))


(def backing-store
  (memory-store))


(def server
  (let [handler (-> (ring-handler backing-store "/blocks/")
                    (wrap-keyword-params)
                    (wrap-params)
                    (wrap-restful-format :formats [:json :edn]))
        options {:server "127.0.0.1"
                 :port 8080
                 :min-threads 2
                 :max-threads 5
                 :max-queued 25
                 :join? false}]
    (jetty/run-jetty handler options)))


(defn start!
  []
  (when-not (.isStarted server)
    (.start server))
  :started)


(defn stop!
  []
  (when (and server (not (.isStopped server)))
    (.stop server))
  :stopped)


(defn reload!
  "Reloads all changed namespaces to update code, then re-launches the system."
  []
  (stop!)
  (refresh :after 'user/start!))
