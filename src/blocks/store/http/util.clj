(ns blocks.store.http.util
  "Shared utility functions."
  (:require
    [blocks.core :as block]
    [clojure.string :as str]
    [multihash.core :as multihash]))


;; ## Date Functions

(defn date-format
  "Constructs a new date formatter matching RFC 2616."
  ^java.text.SimpleDateFormat
  []
  (doto (java.text.SimpleDateFormat. "EEE, dd MMM YYYY HH:mm:ss Z")
    (.setTimeZone (java.util.TimeZone/getTimeZone "GMT"))))


(defn format-date
  "Formats a Java `Date` value into a date string."
  ^String
  [^java.util.Date date]
  (when date
    (.format (date-format) date)))


(defn parse-date
  "Parses a date string into a Java `Date` value."
  ^java.util.Date
  [^String string]
  (when string
    (.parse (date-format) string)))



;; ## Block Metadata Headers

(defn block-headers
  "Constructs a map of metadata headers for a block."
  [block]
  (let [stored-at (or (:stored-at block)
                      (:stored-at (block/meta-stats block)))]
    (cond->
      {"Content-Type" "application/octet-stream"
       "Content-Length" (str (:size block))}
      stored-at
        (assoc "Last-Modified" (format-date stored-at)))))


(defn header-stats
  "Parses a map of headers into stat metadata for a block."
  [headers]
  {:stored-at (parse-date (headers "Last-Modified"))
   :size (some-> (headers "Content-Length") (Integer/parseInt))})
