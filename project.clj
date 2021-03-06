(defproject mvxcvi/blocks-http "0.1.0-SNAPSHOT"
  :description "HTTP transport for content-addressable block storage."
  :url "https://github.com/greglook/blocks-http"
  :license {:name "Public Domain"
            :url "http://unlicense.org/"}

  :deploy-branches ["master"]

  :dependencies
  [[clj-http "2.0.1"]
   [mvxcvi/blocks "0.6.1"]
   [org.clojure/clojure "1.8.0"]
   [org.clojure/tools.logging "0.3.1"]]

  :whidbey
  {:tag-types {'blocks.data.Block {'blocks.data.Block (partial into {})}
               'multihash.core.Multihash {'data/hash 'multihash.core/base58}}}

  :profiles
  {:repl {:source-paths ["dev"]
          :dependencies
          [[ring/ring-jetty-adapter "1.4.0"]
           [ring-middleware-format "0.7.0"]]}})
