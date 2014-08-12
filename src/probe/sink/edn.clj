(ns probe.sink.edn
  "An EDN rolling file sink, largely based on the timbre rolling file sink found here:
  https://github.com/ptaoussanis/timbre/blob/master/src/taoensso/timbre/appenders/rolling.clj"
  (:require [clojure.java.io :as io])
  (:import [java.text SimpleDateFormat]
           [java.util Calendar]))

;; ============================================================================
;; Util

(defn- write-edn
  [f state]
  (spit f (str state "\n") :append true))

;; ============================================================================
;; Rolling

(defn- rename-old-create-new-log [log old-log]
  (.renameTo log old-log)
  (.createNewFile log))

(defn- shift-log-period [log path prev-cal]
  (let [postfix (-> "yyyyMMdd" SimpleDateFormat. (.format (.getTime prev-cal)))
        old-path (format "%s.%s" path postfix)
        old-log (io/file old-path)]
    (if (.exists old-log)
      (loop [index 0]
        (let [index-path (format "%s.%d" old-path index)
              index-log (io/file index-path)]
          (if (.exists index-log)
            (recur (+ index 1))
            (rename-old-create-new-log log index-log))))
      (rename-old-create-new-log log old-log))))

(defn- log-cal [date]
  (let [now (Calendar/getInstance)]
    (.setTime now date)
    now))

(defn- prev-period-end-cal [date pattern]
  (let [cal (log-cal date)
        offset (case pattern
                 :daily 1
                 :weekly (.get cal Calendar/DAY_OF_WEEK)
                 :monthly (.get cal Calendar/DAY_OF_MONTH)
                 0)]
    (.add cal Calendar/DAY_OF_MONTH (* -1 offset))
    (.set cal Calendar/HOUR_OF_DAY 23)
    (.set cal Calendar/MINUTE 59)
    (.set cal Calendar/SECOND 59)
    (.set cal Calendar/MILLISECOND 999)
    cal))

;; ============================================================================
;; Appender

(defn- append [{:keys [ts] :as data} {:keys [path pattern]}]
  (let [prev-cal (prev-period-end-cal ts pattern)
        log (io/file path)]
    (when log
      (try
        (if (.exists log)
          (if (<= (.lastModified log) (.getTimeInMillis prev-cal))
            (shift-log-period log path prev-cal))
          (.createNewFile log))
        (write-edn path data)
        (catch java.io.IOException _)))))

(defn edn-sink
  ([]
   (edn-sink "logs/log.edn" :daily))
  ([path]
   (edn-sink path :daily))
  ([path pattern]
   (assert (#{:daily :weekly :monthly} pattern)
           "Pattern must be one of #{:daily :weekly :monthly}")
   (fn [state]
     (append state {:path path :pattern pattern}))))
