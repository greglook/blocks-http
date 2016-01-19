(defproject mvxcvi/blocks-http "0.1.0-SNAPSHOT"
  :description "HTTP transport for content-addressable block storage."
  :url "https://github.com/greglook/blocks-http"
  :license {:name "Public Domain"
            :url "http://unlicense.org/"}

  :deploy-branches ["master"]

  :dependencies
  [[clj-http "1.1.2"]
   [mvxcvi/blocks "0.6.0"]
   [org.clojure/clojure "1.7.0"]]

  :profiles
  {:repl {:source-paths ["dev"]
          :dependencies
          [[ring/ring-jetty-adapter "1.4.0"]
           [ring-middleware-format "0.6.0"]]}})
