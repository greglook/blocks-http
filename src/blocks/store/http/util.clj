(ns blocks.store.http.util
  "Shared utility functions."
  (:require
    [clojure.string :as str]
    [multihash.core :as multihash]))


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
